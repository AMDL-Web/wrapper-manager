package main

import (
	"archive/zip"
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func wrapperZip(t *testing.T, contents string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	file, err := writer.Create("wrapper")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte(contents)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func TestPrepareWrapperDownloadsMatchingArchitectureAsset(t *testing.T) {
	archives := map[string][]byte{
		"/x86":   wrapperZip(t, "x86-wrapper"),
		"/arm64": wrapperZip(t, "arm64-wrapper"),
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if archive, ok := archives[r.URL.Path]; ok {
			_, _ = w.Write(archive)
			return
		}
		fmt.Fprintf(w, `{"assets":[{"name":"Wrapper.arm64.latest.zip","browser_download_url":"%s/arm64"},{"name":"Wrapper.x86_64.latest.zip","browser_download_url":"%s/x86"}]}`, serverURL(r), serverURL(r))
	}))
	defer server.Close()

	oldBaseURL := wrapperReleaseAPIBaseURL
	wrapperReleaseAPIBaseURL = server.URL + "/"
	defer func() { wrapperReleaseAPIBaseURL = oldBaseURL }()

	for _, test := range []struct {
		arch            string
		wantContents    string
		wantArchivePath string
	}{
		{arch: "amd64", wantContents: "x86-wrapper", wantArchivePath: "wrapper-x86_64.zip"},
		{arch: "arm64", wantContents: "arm64-wrapper", wantArchivePath: "wrapper-arm64.zip"},
	} {
		t.Run(test.arch, func(t *testing.T) {
			oldWorkingDirectory, err := os.Getwd()
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Chdir(t.TempDir()); err != nil {
				t.Fatal(err)
			}
			defer os.Chdir(oldWorkingDirectory)

			if err := prepareWrapper(false, test.arch); err != nil {
				t.Fatal(err)
			}
			contents, err := os.ReadFile(filepath.Join("data", "wrapper", "wrapper"))
			if err != nil {
				t.Fatal(err)
			}
			if string(contents) != test.wantContents {
				t.Fatalf("unexpected wrapper contents: %q", contents)
			}
			if _, err := os.Stat(filepath.Join("data", test.wantArchivePath)); err != nil {
				t.Fatalf("expected architecture archive: %v", err)
			}
			info, err := os.Stat(filepath.Join("data", "wrapper", "wrapper"))
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm()&0o111 == 0 {
				t.Fatal("downloaded wrapper is not executable")
			}
		})
	}
}

func TestDownloadWrapperReleaseRequiresExactAsset(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"assets":[{"name":"Wrapper.x86_64.latest.zip","browser_download_url":"https://example.invalid/x86"}]}`))
	}))
	defer server.Close()

	oldBaseURL := wrapperReleaseAPIBaseURL
	wrapperReleaseAPIBaseURL = server.URL + "/"
	defer func() { wrapperReleaseAPIBaseURL = oldBaseURL }()

	release, err := wrapperReleaseForArch("arm64")
	if err != nil {
		t.Fatal(err)
	}
	err = downloadWrapperRelease(false, release)
	if err == nil || !strings.Contains(err.Error(), release.assetName) {
		t.Fatalf("expected missing arm64 asset error, got %v", err)
	}
}

func TestPrepareWrapperRejectsUnsupportedArchitecture(t *testing.T) {
	err := prepareWrapper(false, "riscv64")
	if err == nil || !strings.Contains(err.Error(), "supports amd64 and arm64") {
		t.Fatalf("expected unsupported architecture error, got %v", err)
	}
}

func serverURL(r *http.Request) string {
	return "http://" + r.Host
}
