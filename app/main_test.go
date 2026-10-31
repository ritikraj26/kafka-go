package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestStartupRejectsCorruptPartitionLog(t *testing.T) {
	if props := os.Getenv("KAFKA_GO_TEST_RECOVERY_CONFIG"); props != "" {
		os.Args = []string{os.Args[0], props}
		main()
		return
	}
	dir := t.TempDir()
	partDir := filepath.Join(dir, "events-0")
	if err := os.MkdirAll(partDir, 0755); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(partDir, "00000000000000000000.log")
	damaged := []byte("incomplete")
	if err := os.WriteFile(logPath, damaged, 0600); err != nil {
		t.Fatal(err)
	}
	assertRecoveryStartupFailure(t, dir)
	after, err := os.ReadFile(logPath)
	if err != nil || !bytes.Equal(after, damaged) {
		t.Fatalf("startup modified the damaged log: %v", err)
	}
}

func TestStartupRejectsMalformedMetadata(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "broker-metadata.json")
	data := []byte(`{"version":1,`)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	assertRecoveryStartupFailure(t, dir)
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, data) {
		t.Fatalf("startup modified malformed metadata: %v", err)
	}
}

func assertRecoveryStartupFailure(t *testing.T, dir string, expectedMessage ...string) {
	t.Helper()
	props := filepath.Join(dir, "server.properties")
	if err := os.WriteFile(props, []byte("log.dirs="+dir+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStartupRejectsCorruptPartitionLog$")
	cmd.Env = append(os.Environ(), "KAFKA_GO_TEST_RECOVERY_CONFIG="+props, "LOG_LEVEL=info")
	output, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if ctx.Err() != nil || !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("expected immediate startup failure: err=%v output=%s", err, output)
	}
	message := "failed to recover partition logs"
	if len(expectedMessage) > 0 {
		message = expectedMessage[0]
	}
	if !bytes.Contains(output, []byte(message)) || bytes.Contains(output, []byte("broker listening")) {
		t.Fatalf("unexpected startup output: %s", output)
	}
}
