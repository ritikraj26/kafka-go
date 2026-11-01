package schema

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSchemaQuotasAndOwnership(t *testing.T) {
	r := NewRegistry()
	defer r.Close()
	limits := DefaultLimits()
	limits.MaxSchemas = 1
	if err := r.SetLimits(limits); err != nil {
		t.Fatal(err)
	}
	s := &SimpleSchema{Fields: map[string]FieldType{"value": TypeString}}
	if err := r.Register("events", s); err != nil {
		t.Fatal(err)
	}
	before := r.Stats().Bytes
	s.Fields["value"] = TypeBoolean
	if r.Get("events").Fields["value"] != TypeString {
		t.Fatal("registration retained the caller's mutable schema")
	}
	copy := r.Get("events")
	copy.Fields["value"] = TypeInteger
	if r.Get("events").Fields["value"] != TypeString {
		t.Fatal("Get exposed mutable retained state")
	}
	if err := r.Register("other", s); !errors.Is(err, ErrQuota) {
		t.Fatal("schema count limit bypassed")
	}
	large := &SimpleSchema{Fields: make(map[string]FieldType)}
	for i := 0; i < limits.MaxFields+1; i++ {
		large.Fields[string(rune(i))] = TypeString
	}
	if err := r.Register("events", large); !errors.Is(err, ErrQuota) {
		t.Fatal("oversized replacement accepted")
	}
	if r.Stats().Bytes != before || r.Get("events").Fields["value"] != TypeString {
		t.Fatal("failed replacement changed state")
	}
	if err := r.Register("events", &SimpleSchema{Fields: map[string]FieldType{}}); err != nil {
		t.Fatal(err)
	}
	if r.Stats().Bytes >= before {
		t.Fatal("smaller replacement did not release bytes")
	}
	r.Close()
	if stats := r.Stats(); stats.Bytes != 0 || stats.Schemas != 0 {
		t.Fatal("close retained schemas")
	}
}

func TestInferenceReservationsAndSampleLimits(t *testing.T) {
	entered := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		io.Copy(io.Discard, req.Body)
		entered <- struct{}{}
		<-req.Context().Done()
	}))
	t.Cleanup(server.Close)
	t.Setenv("OLLAMA_URL", server.URL)
	r := NewRegistry()
	t.Cleanup(r.Close)
	limits := DefaultLimits()
	limits.MaxJobs = 1
	limits.MaxSampleBytes = 32
	if err := r.SetLimits(limits); err != nil {
		t.Fatal(err)
	}
	if err := r.InferAsync("large", make([]byte, 33)); !errors.Is(err, ErrQuota) {
		t.Fatal("oversized sample copied or scheduled")
	}
	if err := r.InferAsync("events", []byte(`{"value":1}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("inference did not start")
	}
	if err := r.InferAsync("other", []byte(`{"value":1}`)); !errors.Is(err, ErrQuota) {
		t.Fatal("inference job ceiling bypassed")
	}
	if stats := r.Stats(); stats.Jobs != 1 || stats.ReservedSchemas != 1 || stats.Bytes != limits.MaxSchemaBytes {
		t.Fatalf("inference reservation: %+v", stats)
	}
	// Explicit registration consumes the reserved slot and cancels inference.
	if err := r.Register("events", &SimpleSchema{Fields: map[string]FieldType{"value": TypeInteger}}); err != nil {
		t.Fatal(err)
	}
	r.workers.Wait()
	if stats := r.Stats(); stats.Jobs != 0 || stats.ReservedSchemas != 0 || stats.Schemas != 1 || stats.Bytes >= limits.MaxSchemaBytes {
		t.Fatalf("reservation not converted/released: %+v", stats)
	}
}
