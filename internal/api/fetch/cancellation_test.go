package fetch

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

func TestLongPollCancellation(t *testing.T) {
	for _, modern := range []bool{false, true} {
		m, topic := fetchFixture(t, 1, 1)
		req := requestFor(topic, 0)
		req.MaxWaitMs = math.MaxInt32
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			var err error
			if modern {
				_, err = BuildBodyContext(ctx, req, m)
			} else {
				_, err = BuildBodyV0Context(ctx, req, m)
			}
			result <- err
		}()
		select {
		case err := <-result:
			cancel()
			t.Fatalf("empty long poll ended before cancellation: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		cancel()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled fetch error=%v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("long poll ignored cancellation")
		}
	}
}
