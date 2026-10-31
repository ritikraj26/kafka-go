package schema

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCloseCancelsAndJoinsInference(t *testing.T) {
	entered, exited := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Error(err)
			return
		}
		close(entered)
		<-r.Context().Done()
		close(exited)
	}))
	t.Cleanup(server.Close)
	t.Setenv("OLLAMA_URL", server.URL)
	reg := NewRegistry()
	t.Cleanup(reg.Close)
	reg.InferAsync("events", []byte(`{"value":"sample"}`))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("inference did not reach HTTP server")
	}
	done := make(chan struct{})
	go func() { reg.Close(); reg.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("registry close waited for HTTP timeout")
	}
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("inference request context was not cancelled")
	}
	if reg.Get("events") != nil {
		t.Fatal("cancelled inference published a fallback schema")
	}
	reg.InferAsync("after-close", []byte(`{"value":1}`))
	if reg.Get("after-close") != nil {
		t.Fatal("closed registry admitted inference")
	}
}
