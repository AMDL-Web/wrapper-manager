package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	pb "github.com/AMDL-Web/wrapper-manager/proto"
	"github.com/gofrs/uuid/v5"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"net"
	"os"
	"os/user"
	"slices"
	"strings"
)

var (
	PROXY                string
	DeviceInfo           string
	Ready                bool
	ShouldStartInstances int
)

type server struct {
	pb.UnimplementedWrapperManagerServiceServer
}

type decryptReplyEnvelope struct {
	reply pb.DecryptReply
	data  pb.DecryptData
}

func newDecryptSuccessReply(header *pb.ReplyHeader, adamID, key string, sampleIndex int32, sample []byte) *pb.DecryptReply {
	envelope := &decryptReplyEnvelope{}
	envelope.reply.Header = header
	envelope.reply.Data = &envelope.data
	envelope.data.AdamId = adamID
	envelope.data.Key = key
	envelope.data.SampleIndex = sampleIndex
	envelope.data.Sample = sample
	return &envelope.reply
}

func (s *server) Status(c context.Context, req *emptypb.Empty) (*pb.StatusReply, error) {
	p, ok := peer.FromContext(c)
	from := "unknown peer"
	if ok {
		from = p.Addr.String()
	}
	// Failed instances are named here because this is the line an operator reads
	// when the pool looks short: an instance the supervisor has given up
	// restarting is missing from ClientCount and would otherwise be silent.
	log.Infof("status request from %s. ClientCount: %d, Ready: %v, ShouldStart: %d, Failed: %s",
		from, len(Instances), Ready, ShouldStartInstances, describeFailedWrappers())
	var regions []string
	var accounts []string
	for _, instance := range Instances {
		if !slices.Contains(regions, instance.Region) {
			regions = append(regions, instance.Region)
		}
		if instance.Account != "" {
			accounts = append(accounts, instance.Account)
		}
	}
	return &pb.StatusReply{
		Header: &pb.ReplyHeader{
			Code: 0,
			Msg:  "SUCCESS",
		},
		Data: &pb.StatusData{
			Status:      len(Instances) != 0,
			Regions:     regions,
			ClientCount: int32(len(Instances)),
			Ready:       Ready,
			Accounts:    accounts,
		},
	}, nil
}

func (s *server) Login(stream grpc.BidiStreamingServer[pb.LoginRequest, pb.LoginReply]) error {
	p, ok := peer.FromContext(stream.Context())
	if ok {
		log.Infof("login stream from %s", p.Addr.String())
	} else {
		log.Infof("login stream from unknown peer")
	}
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if req.Data.TwoStepCode != "" {
			pendingID, ok := PendingLoginByAccount.Load(req.Data.Username)
			if !ok {
				if err = stream.Send(&pb.LoginReply{Header: &pb.ReplyHeader{Code: -1, Msg: "no pending login"}}); err != nil {
					return err
				}
				continue
			}
			id := pendingID.(string)
			provide2FACode(id, req.Data.TwoStepCode)
		} else {
			id, idErr := uuid.NewV4()
			if idErr != nil {
				return idErr
			}
			idString := id.String()
			if _, loaded := PendingLoginByAccount.LoadOrStore(req.Data.Username, idString); loaded {
				if err = stream.Send(&pb.LoginReply{Header: &pb.ReplyHeader{Code: -1, Msg: "login already pending"}}); err != nil {
					return err
				}
				continue
			}
			LoginConnMap.Store(idString, stream)
			go WrapperInitial(id, req.Data.Username, req.Data.Password)
		}
	}
}

func (s *server) Logout(c context.Context, req *pb.LogoutRequest) (*pb.LogoutReply, error) {
	p, ok := peer.FromContext(c)
	if ok {
		log.Infof("logout request from %s", p.Addr.String())
	} else {
		log.Infof("logout request from unknown peer")
	}
	instances := GetInstancesByAccount(req.Data.Username)
	if len(instances) == 0 {
		return &pb.LogoutReply{
			Header: &pb.ReplyHeader{
				Code: -1,
				Msg:  "no such account",
			},
			Data: &pb.LogoutData{Username: req.Data.Username},
		}, nil
	}
	for _, instance := range instances {
		instance.NoRestart = true
		if err := KillWrapper(instance.Id); err != nil {
			return &pb.LogoutReply{
				Header: &pb.ReplyHeader{Code: -1, Msg: "failed to kill wrapper"},
				Data:   &pb.LogoutData{Username: req.Data.Username},
			}, nil
		}
		RemoveWrapperData(instance.Id)
	}
	return &pb.LogoutReply{
		Header: &pb.ReplyHeader{
			Code: 0,
			Msg:  "SUCCESS",
		},
		Data: &pb.LogoutData{Username: req.Data.Username},
	}, nil
}

func (s *server) Decrypt(stream grpc.BidiStreamingServer[pb.DecryptRequest, pb.DecryptReply]) error {
	p, ok := peer.FromContext(stream.Context())
	if ok {
		log.Infof("decrypt stream from %s", p.Addr.String())
	} else {
		log.Infof("decrypt stream from unknown peer")
	}
	var session *DecryptSession
	var sessionAdamId string
	successHeader := &pb.ReplyHeader{Code: 0, Msg: "SUCCESS"}
	defer func() {
		if session != nil {
			session.Close()
		}
	}()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if req == nil || req.Data == nil {
			if err := stream.Send(&pb.DecryptReply{Header: &pb.ReplyHeader{Code: -1, Msg: "missing decrypt data"}}); err != nil {
				return err
			}
			continue
		}
		if req.Data.AdamId == "KEEPALIVE" {
			if err := stream.Send(newDecryptSuccessReply(successHeader, "KEEPALIVE", "", 0, nil)); err != nil {
				return err
			}
			continue
		}
		if session == nil || sessionAdamId != req.Data.AdamId {
			if session != nil {
				session.Close()
			}
			session, err = WMDispatcher.OpenSession(stream.Context(), req.Data.AdamId, req.Data.Key)
			if err != nil {
				if sendErr := stream.Send(decryptErrorReply(req.Data, err)); sendErr != nil {
					return sendErr
				}
				session = nil
				continue
			}
			sessionAdamId = req.Data.AdamId
		}

		result, next, decryptErr := decryptWithFailover(stream.Context(), WMDispatcher, session, req.Data)
		session = next
		if decryptErr != nil {
			if err := stream.Send(&pb.DecryptReply{
				Header: &pb.ReplyHeader{
					Code: -1,
					Msg:  decryptErr.Error(),
				},
				Data: &pb.DecryptData{
					AdamId:      req.Data.AdamId,
					Key:         req.Data.Key,
					SampleIndex: req.Data.SampleIndex,
				},
			}); err != nil {
				return err
			}
		} else {
			if err := stream.Send(newDecryptSuccessReply(
				successHeader,
				req.Data.AdamId,
				req.Data.Key,
				req.Data.SampleIndex,
				result,
			)); err != nil {
				return err
			}
		}
	}
}

// decryptWithFailover decrypts one sample, moving it to a different wrapper
// instance if the one holding the session faults locally. A wedged or reset
// wrapper then costs the client a few extra milliseconds instead of killing the
// track: the caller's stream stays open and still receives a success reply for
// this sample index.
//
// It returns the session to keep using, which is nil once every attempt has
// failed — DecryptSession.Decrypt discards its connection on error, so a faulted
// session is never reusable. Each instance is tried at most once per sample, and
// the error reported is the first fault rather than the placement failure that
// ended the search, because the first one says what actually went wrong.
func decryptWithFailover(ctx context.Context, dispatcher *Dispatcher, session *DecryptSession, data *pb.DecryptData) ([]byte, *DecryptSession, error) {
	result, err := session.Decrypt(data.AdamId, data.Key, data.Sample)
	if err == nil {
		return result, session, nil
	}
	firstErr := err
	tried := make(map[*DecryptInstance]bool)
	for {
		var fault *decryptFault
		if !errors.As(err, &fault) || fault.instance == nil || !fault.replayable {
			return nil, nil, firstErr
		}
		tried[fault.instance] = true
		nextSession, openErr := dispatcher.OpenSessionExcluding(ctx, data.AdamId, data.Key, tried)
		if openErr != nil {
			return nil, nil, firstErr
		}
		log.Warnf(
			"decrypt failover for Adam ID %s: instance %s faulted, retrying on instance %s: %v",
			data.AdamId, fault.instance.id, nextSession.instance.id, err,
		)
		result, err = nextSession.Decrypt(data.AdamId, data.Key, data.Sample)
		if err == nil {
			return result, nextSession, nil
		}
	}
}

func decryptErrorReply(data *pb.DecryptData, err error) *pb.DecryptReply {
	return &pb.DecryptReply{
		Header: &pb.ReplyHeader{Code: -1, Msg: err.Error()},
		Data: &pb.DecryptData{
			AdamId: data.AdamId, Key: data.Key, Sample: data.Sample, SampleIndex: data.SampleIndex,
		},
	}
}

func (s *server) M3U8(c context.Context, req *pb.M3U8Request) (*pb.M3U8Reply, error) {
	p, ok := peer.FromContext(c)
	if ok {
		log.Infof("m3u8 request from %s", p.Addr.String())
	} else {
		log.Infof("m3u8 request from unknown peer")
	}
	instanceID, err := SelectInstance(req.Data.AdamId)
	if err != nil {
		return &pb.M3U8Reply{
			Header: &pb.ReplyHeader{
				Code: -1,
				Msg:  err.Error(),
			},
		}, nil
	}
	if instanceID == "" {
		return &pb.M3U8Reply{
			Header: &pb.ReplyHeader{
				Code: -1,
				Msg:  "no available instance",
			},
		}, nil
	}
	m3u8, err := GetM3U8(GetInstance(instanceID), req.Data.AdamId)
	if err != nil {
		return &pb.M3U8Reply{
			Header: &pb.ReplyHeader{
				Code: -1,
				Msg:  err.Error(),
			},
		}, nil
	}
	if m3u8 == "" {
		return &pb.M3U8Reply{
			Header: &pb.ReplyHeader{
				Code: -1,
				Msg:  fmt.Sprintf("failed to get m3u8 of adamId: %s", req.Data.AdamId),
			},
		}, nil
	}
	return &pb.M3U8Reply{
		Header: &pb.ReplyHeader{
			Code: 0,
			Msg:  "SUCCESS",
		},
		Data: &pb.M3U8DataResponse{
			AdamId: req.Data.AdamId,
			M3U8:   m3u8,
		},
	}, nil
}

func (s *server) Lyrics(c context.Context, req *pb.LyricsRequest) (*pb.LyricsReply, error) {
	p, ok := peer.FromContext(c)
	if ok {
		log.Infof("lyrics request from %s", p.Addr.String())
	} else {
		log.Infof("lyrics request from unknown peer")
	}
	var selectedInstanceId string
	for _, instance := range Instances {
		if strings.ToUpper(instance.Region) == strings.ToUpper(req.Data.Region) {
			selectedInstanceId = instance.Id
		}
	}
	if selectedInstanceId == "" {
		selectedInstanceId = SelectInstanceForLyrics(req.Data.AdamId, req.Data.Language)
		if selectedInstanceId == "" {
			return &pb.LyricsReply{
				Header: &pb.ReplyHeader{
					Code: -1,
					Msg:  "no available instance",
				},
			}, nil
		}
	}
	token, err := GetToken()
	if err != nil {
		return &pb.LyricsReply{
			Header: &pb.ReplyHeader{
				Code: -1,
				Msg:  err.Error(),
			},
		}, nil
	}
	musicToken, err := GetMusicToken(GetInstance(selectedInstanceId))
	if err != nil {
		return &pb.LyricsReply{
			Header: &pb.ReplyHeader{
				Code: -1,
				Msg:  err.Error(),
			},
		}, nil
	}
	inst := GetInstance(selectedInstanceId)
	lyrics, err := GetLyrics(req.Data.AdamId, inst.Region, req.Data.Language, token, musicToken)
	if err != nil {
		return &pb.LyricsReply{
			Header: &pb.ReplyHeader{
				Code: -1,
				Msg:  err.Error(),
			},
		}, nil
	}
	return &pb.LyricsReply{
		Header: &pb.ReplyHeader{
			Code: 0,
			Msg:  "SUCCESS",
		},
		Data: &pb.LyricsDataResponse{
			AdamId: req.Data.AdamId,
			Lyrics: lyrics,
		},
	}, nil
}

func (s *server) WebPlayback(c context.Context, req *pb.WebPlaybackRequest) (*pb.WebPlaybackReply, error) {
	p, ok := peer.FromContext(c)
	if ok {
		log.Infof("webplayback request from %s", p.Addr.String())
	} else {
		log.Infof("webplayback request from unknown peer")
	}
	instanceID, err := SelectInstance(req.Data.AdamId)
	if err != nil {
		return &pb.WebPlaybackReply{
			Header: &pb.ReplyHeader{
				Code: -1,
				Msg:  err.Error(),
			},
			Data: nil,
		}, nil
	}
	if instanceID == "" {
		return &pb.WebPlaybackReply{
			Header: &pb.ReplyHeader{
				Code: -1,
				Msg:  "no available instance",
			},
			Data: nil,
		}, nil
	}
	token, err := GetToken()
	if err != nil {
		return &pb.WebPlaybackReply{
			Header: &pb.ReplyHeader{
				Code: -1,
				Msg:  err.Error(),
			},
			Data: nil,
		}, nil
	}
	musicToken, err := GetMusicToken(GetInstance(instanceID))
	if err != nil {
		return &pb.WebPlaybackReply{
			Header: &pb.ReplyHeader{
				Code: -1,
				Msg:  err.Error(),
			},
			Data: nil,
		}, nil
	}
	m3u8, err := GetWebPlayback(req.Data.AdamId, token, musicToken)
	if err != nil {
		return &pb.WebPlaybackReply{
			Header: &pb.ReplyHeader{
				Code: -1,
				Msg:  err.Error(),
			},
			Data: nil,
		}, nil
	}
	return &pb.WebPlaybackReply{
		Header: &pb.ReplyHeader{
			Code: 0,
			Msg:  "SUCCESS",
		},
		Data: &pb.WebPlaybackDataResponse{
			AdamId: req.Data.AdamId,
			M3U8:   m3u8,
		},
	}, nil
}

func (s *server) License(c context.Context, req *pb.LicenseRequest) (*pb.LicenseReply, error) {
	p, ok := peer.FromContext(c)
	if ok {
		log.Infof("license request from %s", p.Addr.String())
	} else {
		log.Infof("license request from unknown peer")
	}
	instanceID, err := SelectInstance(req.Data.AdamId)
	if err != nil {
		return &pb.LicenseReply{
			Header: &pb.ReplyHeader{
				Code: -1,
				Msg:  err.Error(),
			},
			Data: nil,
		}, nil
	}
	if instanceID == "" {
		return &pb.LicenseReply{
			Header: &pb.ReplyHeader{
				Code: -1,
				Msg:  "no available instance",
			},
			Data: nil,
		}, nil
	}
	token, err := GetToken()
	if err != nil {
		return &pb.LicenseReply{
			Header: &pb.ReplyHeader{
				Code: -1,
				Msg:  err.Error(),
			},
			Data: nil,
		}, nil
	}
	musicToken, err := GetMusicToken(GetInstance(instanceID))
	if err != nil {
		return &pb.LicenseReply{
			Header: &pb.ReplyHeader{
				Code: -1,
				Msg:  err.Error(),
			},
			Data: nil,
		}, nil
	}
	license, renew, err := GetLicense(req.Data.AdamId, req.Data.Challenge, req.Data.Uri, token, musicToken)
	if err != nil {
		return &pb.LicenseReply{
			Header: &pb.ReplyHeader{
				Code: -1,
				Msg:  err.Error(),
			},
			Data: nil,
		}, nil
	}
	return &pb.LicenseReply{
		Header: &pb.ReplyHeader{
			Code: 0,
			Msg:  "SUCCESS",
		},
		Data: &pb.LicenseDataResponse{
			AdamId:  req.Data.AdamId,
			License: license,
			Renew:   int64(renew),
		},
	}, nil
}

func newServer() *server {
	s := &server{}
	return s
}

func main() {
	var host = flag.String("host", "localhost", "host of gRPC server")
	var port = flag.Int("port", 8080, "port of gRPC server")
	var mirror = flag.Bool("mirror", false, "use mirror to download wrapper and file (for Chinese users)")
	var debug = flag.Bool("debug", false, "enable debug output")
	var prepare = flag.Bool("prepare", false, "only download required files")
	var decryptTimeout = flag.Duration("decrypt-timeout", defaultDecryptIOTimeout, "steady-state wrapper decrypt deadline for one sample, not a per-fragment or per-track budget")
	var firstSampleTimeout = flag.Duration("first-sample-timeout", defaultFirstSampleIOTimeout, "deadline for the first decrypt after a context switch, which carries the wrapper's key setup")
	flag.StringVar(&PROXY, "proxy", "", "proxy for wrapper and manager")
	flag.StringVar(&DeviceInfo, "device-info", "Music/5.0.2/Android/10/Pixel 10/7663314/en-US/en-US/dc28071e371c439e", "device info for wrapper")
	flag.Parse()

	if *decryptTimeout <= 0 {
		log.Panicf("-decrypt-timeout must be positive, got %s", *decryptTimeout)
	}
	if *firstSampleTimeout <= 0 {
		log.Panicf("-first-sample-timeout must be positive, got %s", *firstSampleTimeout)
	}
	decryptIOTimeout = *decryptTimeout
	firstSampleIOTimeout = *firstSampleTimeout

	log.SetOutput(os.Stdout)
	if *debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}
	if currentUser.Uid != "0" {
		log.Panicln("root permission required")
	}

	if err := PrepareWrapper(*mirror); err != nil {
		log.Panicf("prepare x86_64 wrapper: %v", err)
	}

	if _, err := os.Stat("data/storefront_ids.json"); errors.Is(err, os.ErrNotExist) {
		log.Warn("storefront ids file dose not exist, downloading...")
		DownloadStorefrontIds()
	}

	if *prepare {
		os.Exit(0)
	}

	WMDispatcher = NewDispatcher()

	Instances = make([]*WrapperInstance, 0)
	if _, err := os.Stat("data/instances.json"); !errors.Is(err, os.ErrNotExist) {
		instancesInFile := LoadInstance()
		ShouldStartInstances = len(instancesInFile)
		if ShouldStartInstances == 0 {
			Ready = true
		} else {
			for _, inst := range instancesInFile {
				go WrapperStart(inst.Id, inst.Account)
			}
		}
	} else {
		ShouldStartInstances = 0
		Ready = true
	}

	log.Printf("wrapperManager running at %s:%d", *host, *port)
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *host, *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	codec, err := newManagerCodec()
	if err != nil {
		log.Fatalf("failed to initialize gRPC codec: %v", err)
	}
	opts = append(opts, grpc.ForceServerCodecV2(codec))
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterWrapperManagerServiceServer(grpcServer, newServer())
	reflection.Register(grpcServer)
	grpcServer.Serve(lis)
}
