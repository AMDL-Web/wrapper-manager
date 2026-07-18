package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"testing"
	"time"
)

func TestWrapperProcessHelper(t *testing.T) {
	mode := os.Getenv("WRAPPER_MANAGER_HELPER_MODE")
	if mode == "" {
		return
	}
	if mode == "ignore" {
		signal.Ignore(os.Interrupt)
		fmt.Println("ready")
		select {}
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	fmt.Println("ready")
	<-interrupt
}

func startWrapperProcessHelper(t *testing.T, mode string) *WrapperInstance {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestWrapperProcessHelper")
	cmd.Env = append(os.Environ(), "WRAPPER_MANAGER_HELPER_MODE="+mode)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if scanner := bufio.NewScanner(stdout); !scanner.Scan() || scanner.Text() != "ready" {
		_ = cmd.Process.Kill()
		t.Fatal("wrapper process helper did not become ready")
	}
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		if cmd.ProcessState == nil || !cmd.ProcessState.Exited() {
			_ = cmd.Process.Kill()
		}
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	})
	return &WrapperInstance{Id: "helper", Cmd: cmd, Done: done}
}

func TestTerminateHungWrapperEscalatesToKill(t *testing.T) {
	instance := startWrapperProcessHelper(t, "ignore")
	started := time.Now()
	if err := terminateWrapperInstance(instance, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 50*time.Millisecond {
		t.Fatalf("hung wrapper terminated before grace period elapsed: %s", elapsed)
	}
	select {
	case <-instance.Done:
	case <-time.After(time.Second):
		t.Fatal("hung wrapper process did not exit after SIGKILL")
	}
}

func TestTerminateResponsiveWrapperExitsDuringGrace(t *testing.T) {
	instance := startWrapperProcessHelper(t, "responsive")
	if err := terminateWrapperInstance(instance, time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case <-instance.Done:
	case <-time.After(time.Second):
		t.Fatal("responsive wrapper process did not exit after SIGINT")
	}
}
