package main

import (
	"bytes"
	"math"
	"sync"
	"testing"

	pb "github.com/AMDL-Web/wrapper-manager/proto"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
)

func TestManagerCodecDecryptSuccessMatchesStandardProtobuf(t *testing.T) {
	tests := []struct {
		name  string
		reply *pb.DecryptReply
	}{
		{
			name: "typical",
			reply: newDecryptSuccessReply(
				&pb.ReplyHeader{Code: 0, Msg: "SUCCESS"},
				"1597687743", "skd://key", 42, bytes.Repeat([]byte{0xAB}, 256<<10),
			),
		},
		{
			name: "zero index and empty metadata",
			reply: newDecryptSuccessReply(
				&pb.ReplyHeader{Code: 0, Msg: "SUCCESS"},
				"", "", 0, []byte{1},
			),
		},
		{
			name: "negative index",
			reply: newDecryptSuccessReply(
				&pb.ReplyHeader{Code: 0, Msg: "SUCCESS"},
				"song", "key", -1, []byte{1, 2, 3},
			),
		},
		{
			name: "maximum index",
			reply: newDecryptSuccessReply(
				&pb.ReplyHeader{Code: 0, Msg: "SUCCESS"},
				string(bytes.Repeat([]byte{'a'}, 300)), string(bytes.Repeat([]byte{'k'}, 300)), math.MaxInt32, []byte{4, 5, 6},
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buffers, fast := marshalDecryptSuccess(test.reply)
			if !fast {
				t.Fatal("success reply did not use fast codec path")
			}
			got := buffers.Materialize()
			buffers.Free()
			want, err := proto.Marshal(test.reply)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("wire mismatch: got %x, want %x", got, want)
			}
			decoded := new(pb.DecryptReply)
			if err := proto.Unmarshal(got, decoded); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(decoded, test.reply) {
				t.Fatalf("decoded reply differs: got %v, want %v", decoded, test.reply)
			}
		})
	}
}

func TestManagerCodecFallsBackForNonCanonicalReply(t *testing.T) {
	tests := []*pb.DecryptReply{
		nil,
		{},
		{Header: &pb.ReplyHeader{Code: -1, Msg: "failed"}, Data: &pb.DecryptData{Sample: []byte{1}}},
		{Header: &pb.ReplyHeader{Code: 0, Msg: "SUCCESS"}, Data: &pb.DecryptData{}},
		{Header: &pb.ReplyHeader{Code: 0, Msg: "different"}, Data: &pb.DecryptData{Sample: []byte{1}}},
	}
	withUnknown := newDecryptSuccessReply(
		&pb.ReplyHeader{Code: 0, Msg: "SUCCESS"}, "song", "key", 1, []byte{1},
	)
	withUnknown.ProtoReflect().SetUnknown([]byte{0x98, 0x06, 0x01})
	tests = append(tests, withUnknown)

	codec, err := newManagerCodec()
	if err != nil {
		t.Fatal(err)
	}
	for i, reply := range tests {
		if _, fast := marshalDecryptSuccess(reply); fast {
			t.Fatalf("case %d unexpectedly used fast path", i)
		}
		gotBuffers, err := codec.Marshal(reply)
		if err != nil {
			if reply == nil {
				continue
			}
			t.Fatalf("case %d: %v", i, err)
		}
		got := gotBuffers.Materialize()
		gotBuffers.Free()
		want, err := proto.Marshal(reply)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("case %d fallback wire mismatch", i)
		}
	}
}

func TestManagerCodecConcurrentMarshal(t *testing.T) {
	codec, err := newManagerCodec()
	if err != nil {
		t.Fatal(err)
	}
	const workers = 32
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			reply := newDecryptSuccessReply(
				&pb.ReplyHeader{Code: 0, Msg: "SUCCESS"},
				"song", "key", int32(index), bytes.Repeat([]byte{byte(index)}, 64<<10),
			)
			buffers, marshalErr := codec.Marshal(reply)
			if marshalErr != nil {
				t.Errorf("marshal: %v", marshalErr)
				return
			}
			wire := buffers.Materialize()
			buffers.Free()
			decoded := new(pb.DecryptReply)
			if unmarshalErr := codec.Unmarshal(mem.BufferSlice{mem.SliceBuffer(wire)}, decoded); unmarshalErr != nil {
				t.Errorf("unmarshal: %v", unmarshalErr)
				return
			}
			if !proto.Equal(decoded, reply) {
				t.Errorf("decoded reply differs for worker %d", index)
			}
		}(worker)
	}
	wg.Wait()
}

func BenchmarkDecryptReplyMarshal(b *testing.B) {
	reply := newDecryptSuccessReply(
		&pb.ReplyHeader{Code: 0, Msg: "SUCCESS"},
		"1597687743", "skd://key", 42, bytes.Repeat([]byte{0xAB}, 256<<10),
	)
	codec, err := newManagerCodec()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(reply.Data.Sample)))
	b.Run("standard", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			wire, marshalErr := proto.Marshal(reply)
			if marshalErr != nil {
				b.Fatal(marshalErr)
			}
			_ = wire
		}
	})
	b.Run("split", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buffers, marshalErr := codec.Marshal(reply)
			if marshalErr != nil {
				b.Fatal(marshalErr)
			}
			buffers.Free()
		}
	})
}
