package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestStartupRejectsCorruptConsumerOffsets(t *testing.T) {
	dir := t.TempDir()
	offsetDir := filepath.Join(dir, "__consumer_offsets-0")
	if err := os.Mkdir(offsetDir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(offsetDir, "00000000000000000000.log")
	damaged := []byte("{\"group\":\"g\",\"topic\":\"events\",\"partition\":0,\"offset\":")
	if err := os.WriteFile(path, damaged, 0600); err != nil {
		t.Fatal(err)
	}
	assertRecoveryStartupFailure(t, dir, "failed to recover consumer offsets")
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, damaged) {
		t.Fatalf("startup altered damaged offsets: %v", err)
	}
}
