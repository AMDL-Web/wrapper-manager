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

func wrapperZip(t *testing.T) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	file, err := writer.Create("wrapper")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("x86-wrapper")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func TestPrepareWrapperDownloadsExactX86Asset(t *testing.T) {
	archive := wrapperZip(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/release":
			fmt.Fprintf(w, `{"assets":[{"name":"Wrapper.arm64.latest.zip","browser_download_url":"%s/arm64"},{"name":"%s","browser_download_url":"%s/x86"}]}`, serverURL(r), wrapperAssetName, serverURL(r))
		case "/x86":
			_, _ = w.Write(archive)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	oldURL := wrapperReleaseAPIURL
	wrapperReleaseAPIURL = server.URL + "/release"
	defer func() { wrapperReleaseAPIURL = oldURL }()

	oldWorkingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldWorkingDirectory)

	if err := prepareWrapper(false, "amd64"); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join("data", "wrapper", "wrapper"))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "x86-wrapper" {
		t.Fatalf("unexpected wrapper contents: %q", contents)
	}
	info, err := os.Stat(filepath.Join("data", "wrapper", "wrapper"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Fatal("downloaded wrapper is not executable")
	}
}

func TestDownloadWrapperReleaseRequiresExactAsset(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"assets":[{"name":"Wrapper.arm64.latest.zip","browser_download_url":"https://example.invalid/arm64"}]}`))
	}))
	defer server.Close()

	oldURL := wrapperReleaseAPIURL
	wrapperReleaseAPIURL = server.URL
	defer func() { wrapperReleaseAPIURL = oldURL }()

	err := DownloadWrapperRelease(false)
	if err == nil || !strings.Contains(err.Error(), wrapperAssetName) {
		t.Fatalf("expected missing x86 asset error, got %v", err)
	}
}

func TestPrepareWrapperRejectsNonX86(t *testing.T) {
	err := prepareWrapper(false, "arm64")
	if err == nil || !strings.Contains(err.Error(), "only supports x86_64") {
		t.Fatalf("expected unsupported architecture error, got %v", err)
	}
}

func serverURL(r *http.Request) string {
	return "http://" + r.Host
}
