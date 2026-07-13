package main

import (
	pb "github.com/AMDL-Web/wrapper-manager/proto"
	"google.golang.org/grpc"
	"log"
	"sync"
)

var LoginConnMap = sync.Map{}
var PendingLoginByAccount = sync.Map{}

func Login2FAHandler(id string) {
	conn, _ := LoginConnMap.Load(id)
	err := conn.(grpc.BidiStreamingServer[pb.LoginRequest, pb.LoginReply]).Send(
		&pb.LoginReply{
			Header: &pb.ReplyHeader{
				Code: 2,
				Msg:  "2fa code require",
			},
		})
	if err != nil {
		log.Println(err)
	}
}

func LoginDoneHandler(id string) {
	SaveInstances()
	clearPendingLogin(id)
	conn, _ := LoginConnMap.LoadAndDelete(id)
	if conn == nil {
		return
	}
	err := conn.(grpc.BidiStreamingServer[pb.LoginRequest, pb.LoginReply]).Send(
		&pb.LoginReply{
			Header: &pb.ReplyHeader{
				Code: 0,
				Msg:  "SUCCESS",
			},
		})
	if err != nil {
		log.Println(err)
	}
}

func LoginFailedHandler(id string) {
	RemoveWrapperData(id)
	clearPendingLogin(id)
	conn, _ := LoginConnMap.LoadAndDelete(id)
	if conn == nil {
		return
	}
	err := conn.(grpc.BidiStreamingServer[pb.LoginRequest, pb.LoginReply]).Send(
		&pb.LoginReply{
			Header: &pb.ReplyHeader{
				Code: -1,
				Msg:  "login failed",
			},
		})
	if err != nil {
		log.Println(err)
	}
}

func clearPendingLogin(id string) {
	PendingLoginByAccount.Range(func(account, pendingID any) bool {
		if pendingID == id {
			PendingLoginByAccount.Delete(account)
			return false
		}
		return true
	})
}
