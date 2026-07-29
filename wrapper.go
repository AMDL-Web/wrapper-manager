package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/artdarek/go-unzip"
	"github.com/creack/pty"
	"github.com/gofrs/uuid/v5"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type wrapperRelease struct {
	tag         string
	assetName   string
	archivePath string
}

var wrapperReleaseAPIBaseURL = "https://api.github.com/repos/AMDL-Web/wrapper/releases/tags/"

func wrapperReleaseForArch(arch string) (wrapperRelease, error) {
	switch arch {
	case "amd64":
		return wrapperRelease{
			tag:         "wrapper.x86_64.latest",
			assetName:   "Wrapper.x86_64.latest.zip",
			archivePath: "data/wrapper-x86_64.zip",
		}, nil
	case "arm64":
		return wrapperRelease{
			tag:         "wrapper.arm64.latest",
			assetName:   "Wrapper.arm64.latest.zip",
			archivePath: "data/wrapper-arm64.zip",
		}, nil
	default:
		return wrapperRelease{}, fmt.Errorf("wrapper auto-install only supports amd64 and arm64, current architecture is %s", arch)
	}
}

func parseStorefrontID(id string) string {
	sfID, err := strconv.Atoi(strings.Split(id, "-")[0])
	if err != nil {
		panic(err)
	}
	type StorefrontMapping struct {
		Name         string `json:"name"`
		Code         string `json:"code"`
		StorefrontId int    `json:"storefrontId"`
	}
	var mapping []StorefrontMapping
	file, err := os.ReadFile("data/storefront_ids.json")
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(file, &mapping)
	if err != nil {
		panic(err)
	}
	for _, element := range mapping {
		if element.StorefrontId == sfID {
			return element.Code
		}
	}
	return ""
}

func PrepareWrapper(mirror bool) error {
	return prepareWrapper(mirror, runtime.GOARCH)
}

func prepareWrapper(mirror bool, arch string) error {
	release, err := wrapperReleaseForArch(arch)
	if err != nil {
		return err
	}
	if _, err := os.Stat("data/wrapper/wrapper"); os.IsNotExist(err) {
		if _, err := os.Stat(release.archivePath); os.IsNotExist(err) {
			if err := downloadWrapperRelease(mirror, release); err != nil {
				return err
			}
		}
		err := unzip.New(release.archivePath, "data/wrapper").Extract()
		if err != nil {
			return fmt.Errorf("extract wrapper release: %w", err)
		}
		err = os.Chmod("data/wrapper/wrapper", 0777)
		if err != nil {
			return fmt.Errorf("make wrapper executable: %w", err)
		}
	}
	return nil
}

func WrapperInitial(id uuid.UUID, account string, password string) {
	err := os.MkdirAll("data/wrapper/rootfs/data/instances/"+id.String(), 0777)
	if err != nil {
		panic(err)
	}

	err = os.WriteFile("data/wrapper/rootfs/data/instances/"+id.String()+"/ACCOUNT", []byte(account), 0777)
	if err != nil {
		log.Warnf("failed to write ACCOUNT file: %v", err)
	}

	instance := WrapperInstance{
		Id:          id.String(),
		Account:     account,
		DecryptPort: GenerateUniquePort(),
		M3U8Port:    GenerateUniquePort(),
		NoRestart:   true,
		Done:        make(chan struct{}),
		proc:        newWrapperProc(),
	}

	args := []string{
		"-H0.0.0.0",
		fmt.Sprintf("-L%s:%s", account, password),
		fmt.Sprintf("-B%s", "/data/instances/"+instance.Id),
		fmt.Sprintf("-D%d", instance.DecryptPort),
		fmt.Sprintf("-M%d", instance.M3U8Port),
		fmt.Sprintf("-I%s", DeviceInfo),
		"-F",
	}

	if PROXY != "" {
		args = append(args, fmt.Sprintf("-P%s", PROXY))
	}

	cmd := exec.Command("./wrapper", args...)
	cmd.Dir = "data/wrapper/"

	ptmx, err := pty.Start(cmd)
	if err != nil {
		panic(err)
	}
	defer func() { _ = ptmx.Close() }()

	instance.Cmd = cmd
	instance.proc.markStarted(time.Now())
	go handleOutput(ptmx, &instance)

	waitErr := cmd.Wait()
	close(instance.Done)

	go wrapperDown(&instance, waitErr)
}

func WrapperStart(id string, account string) {
	if account == "" {
		accountBytes, err := os.ReadFile("data/wrapper/rootfs/data/instances/" + id + "/ACCOUNT")
		if err == nil {
			account = string(accountBytes)
		}
	}
	instance := WrapperInstance{
		Id:          id,
		Account:     account,
		DecryptPort: GenerateUniquePort(),
		M3U8Port:    GenerateUniquePort(),
		NoRestart:   false,
		Done:        make(chan struct{}),
		proc:        newWrapperProc(),
	}

	args := []string{
		"-H0.0.0.0",
		fmt.Sprintf("-B%s", "/data/instances/"+id),
		fmt.Sprintf("-D%d", instance.DecryptPort),
		fmt.Sprintf("-M%d", instance.M3U8Port),
		fmt.Sprintf("-I%s", DeviceInfo),
	}

	if PROXY != "" {
		args = append(args, fmt.Sprintf("-P%s", PROXY))
	}

	cmd := exec.Command("./wrapper", args...)
	cmd.Dir = "data/wrapper/"

	ptmx, err := pty.Start(cmd)
	if err != nil {
		panic(err)
	}
	defer func() { _ = ptmx.Close() }()

	instance.Cmd = cmd
	instance.proc.markStarted(time.Now())
	go handleOutput(ptmx, &instance)

	waitErr := cmd.Wait()
	close(instance.Done)

	go wrapperDown(&instance, waitErr)
}

// handleOutput is the only place the wrapper's own words reach the manager. It
// acts on four fixed markers; everything else used to be dropped after a debug
// line that production never printed. Now every line is kept in the instance's
// tail so a death can quote it, and logWrapperLine decides its level.
func handleOutput(reader io.Reader, instance *WrapperInstance) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		instance.proc.recordLine(line)
		logWrapperLine(instance, line)

		if strings.Contains(line, "Waiting for input...") {
			go Login2FAHandler(instance.Id)
		}
		if strings.Contains(line, "[!] listening m3u8 request on") {
			go wrapperReady(instance)
		}
		if strings.Contains(line, "[!] login failed") {
			go LoginFailedHandler(instance.Id)
		}
		if strings.Contains(line, "No Active Subscription") {
			go NoSubscriptionHandler(instance)
		}
	}
}

func wrapperReady(instance *WrapperInstance) {
	wrapperFailures.clear(instance.Id)
	storefrontID, err := os.ReadFile(fmt.Sprintf("data/wrapper/rootfs/data/instances/%s/STOREFRONT_ID", instance.Id))
	if err != nil {
		panic(err)
	}
	region := parseStorefrontID(string(storefrontID))
	instance.Region = region
	InsertInstance(instance)
	WMDispatcher.AddInstance(instance)
	instance.NoRestart = false
	go LoginDoneHandler(instance.Id)
	log.Infof("[wrapper %s] Wrapper ready. len(Instances)=%d, ShouldStartInstances=%d", strings.Split(instance.Id, "-")[0], len(Instances), ShouldStartInstances)
	if len(Instances) >= ShouldStartInstances {
		Ready = true
	}
}

// wrapperDown runs once per wrapper death. It records what happened, then
// decides whether restarting is still worth doing: the previous version
// restarted unconditionally, which on 2026-07-29 meant four restarts in 52
// seconds into the same immediate death, invisibly.
func wrapperDown(instance *WrapperInstance, waitErr error) {
	logWrapperExit(instance, waitErr)
	tag := wrapperTag(instance.Id)
	ReleasePort(instance.DecryptPort)
	ReleasePort(instance.M3U8Port)
	RemoveInstance(instance)
	if WMDispatcher != nil {
		WMDispatcher.RemoveInstance(instance.Id)
	}

	if instance.NoRestart {
		abandonReplacement(instance.Id, "instance is not configured to restart")
		SaveInstances()
		return
	}

	now := time.Now()
	decision := wrapperRestarts.plan(instance.Id, instance.proc.uptime(now), now)
	if !decision.restart {
		reason := fmt.Sprintf(
			"crash loop: %d deaths in a row without staying up %s, %d restarts already spent within %s",
			decision.deaths, wrapperHealthyUptime, wrapperRestartLimit, wrapperRestartWindow,
		)
		log.Errorf("%s not restarting it again — %s; the instance is marked failed and needs attention", tag, reason)
		wrapperFailures.mark(WrapperFailure{
			Id:       instance.Id,
			Account:  instance.Account,
			Reason:   reason,
			At:       now,
			Deaths:   decision.deaths,
			Restarts: wrapperRestartLimit,
		})
		// A wedged instance elsewhere in the pool may be held in service waiting
		// for this replacement. It is not coming.
		abandonReplacement(instance.Id, reason)
		return
	}
	if decision.delay > 0 {
		log.Warnf("%s died after less than %s, %d times in a row; restarting in %s (%d of %d restarts spent)",
			tag, wrapperHealthyUptime, decision.deaths, decision.delay, decision.deaths, wrapperRestartLimit)
	} else if decision.shortLived {
		log.Warnf("%s died after less than %s; restarting now", tag, wrapperHealthyUptime)
	} else {
		log.Infof("%s restarting now", tag)
	}
	startWrapper(instance.Id, instance.Account, decision.delay)
}

func KillWrapper(id string) error {
	instance := GetInstance(id)
	if instance == nil || instance.Id == "" {
		return fmt.Errorf("instance %s not found", id)
	}
	return signalWrapperInstance(instance)
}

func signalWrapperInstance(instance *WrapperInstance) error {
	if instance == nil {
		return fmt.Errorf("wrapper instance is nil")
	}
	if instance.Cmd == nil {
		return fmt.Errorf("instance %s cmd is nil", instance.Id)
	}
	if instance.Cmd.Process == nil {
		return fmt.Errorf("instance %s process is nil", instance.Id)
	}
	// Recorded so the exit report can say whether anyone asked for this death.
	// A wrapper leaving with status 0 that nobody signalled is the shape of the
	// fault this manager exists to catch.
	instance.proc.markStopRequested()
	// Send SIGINT to trigger wrapper's internal child-killing signal handler
	err := instance.Cmd.Process.Signal(os.Interrupt)
	if err != nil {
		return instance.Cmd.Process.Kill()
	}
	return nil
}

func terminateWrapperInstance(instance *WrapperInstance, grace time.Duration) error {
	if err := signalWrapperInstance(instance); err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			return nil
		}
		return err
	}
	if instance.Done == nil {
		return nil
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-instance.Done:
		return nil
	case <-timer.C:
	}
	log.Warnf("[wrapper %s] did not exit within %s after SIGINT; sending SIGKILL", strings.Split(instance.Id, "-")[0], grace)
	if err := instance.Cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	timer.Reset(grace)
	select {
	case <-instance.Done:
		return nil
	case <-timer.C:
		return fmt.Errorf("instance %s did not exit after SIGKILL", instance.Id)
	}
}

func provide2FACode(id string, code string) {
	path := "data/wrapper/rootfs/data/instances/" + id + "/2fa.txt"
	err := os.WriteFile(path, []byte(code), 0777)
	if err != nil {
		log.Warnf("failed to write 2fa.txt: %v", err)
	}
}

func RemoveWrapperData(id string) {
	err := os.RemoveAll("data/wrapper/rootfs/data/instances/" + id)
	if err != nil {
		panic(err)
	}
}

func DownloadWrapperRelease(mirror bool) error {
	release, err := wrapperReleaseForArch(runtime.GOARCH)
	if err != nil {
		return err
	}
	return downloadWrapperRelease(mirror, release)
}

func downloadWrapperRelease(mirror bool, expected wrapperRelease) error {
	resp, err := GetHttpClient().Get(wrapperReleaseAPIBaseURL + expected.tag)
	if err != nil {
		return fmt.Errorf("request wrapper release: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("request wrapper release: %s", resp.Status)
	}

	var release struct {
		Assets []struct {
			Name               string `json:"name"`
			BrowserDownloadURL string `json:"browser_download_url"`
		} `json:"assets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return fmt.Errorf("decode wrapper release: %w", err)
	}

	var downloadURL string
	for _, asset := range release.Assets {
		if asset.Name == expected.assetName {
			downloadURL = asset.BrowserDownloadURL
			break
		}
	}
	if downloadURL == "" {
		return fmt.Errorf("wrapper release %s has no %s asset", expected.tag, expected.assetName)
	}
	if mirror {
		downloadURL = strings.Replace(downloadURL, "github.com", "gh-proxy.com/github.com", 1)
	}

	wrapperResp, err := GetHttpClient().Get(downloadURL)
	if err != nil {
		return fmt.Errorf("download wrapper release: %w", err)
	}
	defer wrapperResp.Body.Close()
	if wrapperResp.StatusCode < 200 || wrapperResp.StatusCode >= 300 {
		return fmt.Errorf("download wrapper release: %s", wrapperResp.Status)
	}
	if err := os.MkdirAll("data", 0o755); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}
	temp, err := os.CreateTemp("data", "wrapper-*.zip")
	if err != nil {
		return fmt.Errorf("create wrapper download: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if _, err := io.Copy(temp, wrapperResp.Body); err != nil {
		temp.Close()
		return fmt.Errorf("save wrapper release: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close wrapper release: %w", err)
	}
	if err := os.Rename(tempPath, expected.archivePath); err != nil {
		return fmt.Errorf("finalize wrapper release: %w", err)
	}
	return nil
}

func DownloadStorefrontIds() {
	resp, err := GetHttpClient().Get("https://gist.githubusercontent.com/BrychanOdlum/2208578ba151d1d7c4edeeda15b4e9b1/raw/8f01e4a4cb02cf97a48aba4665286b0e8de14b8e/storefrontmappings.json")
	if err != nil {
		panic(err)
	}
	ids, err := io.ReadAll(resp.Body)
	err = os.WriteFile("data/storefront_ids.json", ids, 0777)
	if err != nil {
		panic(err)
	}
}

func NoSubscriptionHandler(instance *WrapperInstance) {
	if instance.NoRestart {
		go LoginFailedHandler(instance.Id)
	} else {
		RemoveInstance(instance)
		RemoveWrapperData(instance.Id)
		SaveInstances()
	}
}
