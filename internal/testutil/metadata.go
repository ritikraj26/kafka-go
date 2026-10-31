// Package testutil provides disk-isolated broker fixtures.
package testutil

import (
	"testing"

	"github.com/ritiraj/kafka-go/internal/metadata"
)

func NewMetadataManager(t testing.TB) *metadata.Manager {
	t.Helper()
	m := metadata.NewManager()
	if err := m.SetLogDir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Schemas.Close)
	return m
}

func CreateTopic(t testing.TB, m *metadata.Manager, name string, partitions int) *metadata.Topic {
	t.Helper()
	topic, err := m.CreateTopic(name, partitions)
	if err != nil {
		t.Fatal(err)
	}
	return topic
}
