package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestMinimumISRConfiguration(t *testing.T) {
	original, present := os.LookupEnv("MIN_INSYNC_REPLICAS")
	if err := os.Unsetenv("MIN_INSYNC_REPLICAS"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if present {
			if err := os.Setenv("MIN_INSYNC_REPLICAS", original); err != nil {
				t.Error(err)
			}
		} else if err := os.Unsetenv("MIN_INSYNC_REPLICAS"); err != nil {
			t.Error(err)
		}
	})
	if n, err := minInSyncReplicasFromEnv(); err != nil || n != 2 {
		t.Fatalf("default minimum ISR=%d err=%v", n, err)
	}
	for _, tc := range []struct {
		value string
		want  int
	}{
		{"1", 1}, {"2", 2}, {"3", 3}, {"", 0}, {"0", 0}, {"-1", 0}, {"two", 0}, {"2147483648", 0},
	} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("MIN_INSYNC_REPLICAS", tc.value)
			got, err := minInSyncReplicasFromEnv()
			if tc.want == 0 {
				if err == nil {
					t.Fatal("invalid minimum ISR accepted")
				}
			} else if err != nil || got != tc.want {
				t.Fatalf("got=%d err=%v, want %d", got, err, tc.want)
			}
		})
	}
}

func TestStartupRejectsInvalidMinimumISR(t *testing.T) {
	if os.Getenv("KAFKA_GO_TEST_INVALID_ISR") == "1" {
		os.Args = os.Args[:1]
		main()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStartupRejectsInvalidMinimumISR$")
	cmd.Env = append(os.Environ(), "KAFKA_GO_TEST_INVALID_ISR=1", "MIN_INSYNC_REPLICAS=invalid", "LOG_LEVEL=info")
	output, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if ctx.Err() != nil || !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("expected early startup failure: err=%v output=%s", err, output)
	}
	if !bytes.Contains(output, []byte("invalid minimum ISR configuration")) || bytes.Contains(output, []byte("broker listening")) {
		t.Fatalf("unexpected startup output: %s", output)
	}
}
