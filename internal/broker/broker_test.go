package broker

import (
	"testing"
)

func TestRegistry_RegisterAndGet(t *testing.T) {
	reg := NewRegistry(1)
	reg.Register(&Broker{ID: 1, Host: "localhost", Port: 9092})
	reg.Register(&Broker{ID: 2, Host: "localhost", Port: 9093})

	b := reg.Get(1)
	if b == nil {
		t.Fatal("expected broker 1, got nil")
	}
	if b.Host != "localhost" || b.Port != 9092 {
		t.Errorf("broker 1: got %v:%v, want localhost:9092", b.Host, b.Port)
	}
	if reg.Get(99) != nil {
		t.Error("expected nil for unknown broker 99")
	}
}

func TestRegistry_Local(t *testing.T) {
	reg := NewRegistry(2)
	reg.Register(&Broker{ID: 1, Host: "a", Port: 9092})
	reg.Register(&Broker{ID: 2, Host: "b", Port: 9093})
	local := reg.Local()
	if local == nil {
		t.Fatal("Local() returned nil")
	}
	if local.ID != 2 {
		t.Errorf("Local().ID = %d, want 2", local.ID)
	}
}

func TestRegistry_All(t *testing.T) {
	reg := NewRegistry(1)
	reg.Register(&Broker{ID: 1, Host: "a", Port: 9092})
	reg.Register(&Broker{ID: 2, Host: "b", Port: 9093})
	reg.Register(&Broker{ID: 3, Host: "c", Port: 9094})
	all := reg.All()
	if len(all) != 3 {
		t.Errorf("All() len = %d, want 3", len(all))
	}
}

func TestRegistry_IDs(t *testing.T) {
	reg := NewRegistry(1)
	reg.Register(&Broker{ID: 10, Host: "a", Port: 9092})
	reg.Register(&Broker{ID: 20, Host: "b", Port: 9093})
	ids := reg.IDs()
	if len(ids) != 2 {
		t.Errorf("IDs() len = %d, want 2", len(ids))
	}
	found := map[int32]bool{}
	for _, id := range ids {
		found[id] = true
	}
	if !found[10] || !found[20] {
		t.Errorf("IDs() = %v, want [10 20]", ids)
	}
}

func TestRegistry_RegisterOverwrite(t *testing.T) {
	reg := NewRegistry(1)
	reg.Register(&Broker{ID: 1, Host: "old", Port: 9092})
	reg.Register(&Broker{ID: 1, Host: "new", Port: 9999})
	b := reg.Get(1)
	if b.Host != "new" || b.Port != 9999 {
		t.Errorf("expected overwritten broker, got %v:%v", b.Host, b.Port)
	}
	if len(reg.All()) != 1 {
		t.Errorf("expected 1 broker after overwrite, got %d", len(reg.All()))
	}
}
