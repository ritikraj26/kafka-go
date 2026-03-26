package schema

import "testing"

func TestValidate_NoSchema(t *testing.T) {
	r := NewRegistry()
	if err := r.Validate("orders", []byte(`{"order_id":"abc","amount":9.99}`)); err != nil {
		t.Fatalf("expected nil for unknown topic, got %v", err)
	}
}

func TestValidate_ValidMessage(t *testing.T) {
	r := NewRegistry()
	r.Register("orders", &SimpleSchema{Fields: map[string]FieldType{
		"order_id": TypeString,
		"amount":   TypeNumber,
	}})
	if err := r.Validate("orders", []byte(`{"order_id":"abc","amount":9.99}`)); err != nil {
		t.Fatalf("expected valid message to pass, got %v", err)
	}
}

func TestValidate_WrongType(t *testing.T) {
	r := NewRegistry()
	r.Register("orders", &SimpleSchema{Fields: map[string]FieldType{
		"order_id": TypeString,
	}})
	if err := r.Validate("orders", []byte(`{"order_id":123}`)); err == nil {
		t.Fatal("expected type mismatch error, got nil")
	}
}

func TestValidate_MissingField(t *testing.T) {
	r := NewRegistry()
	r.Register("orders", &SimpleSchema{Fields: map[string]FieldType{
		"order_id": TypeString,
		"amount":   TypeNumber,
	}})
	if err := r.Validate("orders", []byte(`{"order_id":"abc"}`)); err == nil {
		t.Fatal("expected missing field error, got nil")
	}
}

func TestValidate_NotJSON(t *testing.T) {
	r := NewRegistry()
	r.Register("events", &SimpleSchema{Fields: map[string]FieldType{
		"type": TypeString,
	}})
	if err := r.Validate("events", []byte("not json at all")); err == nil {
		t.Fatal("expected JSON parse error, got nil")
	}
}

func TestValidate_IntegerAllowedAsNumber(t *testing.T) {
	r := NewRegistry()
	r.Register("payments", &SimpleSchema{Fields: map[string]FieldType{
		"amount": TypeNumber,
	}})
	if err := r.Validate("payments", []byte(`{"amount":42}`)); err != nil {
		t.Fatalf("expected integer to satisfy number type, got %v", err)
	}
}

func TestInferFromJSON(t *testing.T) {
	payload := []byte(`{"name":"alice","age":30,"active":true,"score":9.5}`)
	s, err := InferFromJSON(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	checks := map[string]FieldType{
		"name":   TypeString,
		"age":    TypeInteger,
		"active": TypeBoolean,
		"score":  TypeNumber,
	}
	for field, want := range checks {
		got, ok := s.Fields[field]
		if !ok {
			t.Errorf("field %q not in inferred schema", field)
			continue
		}
		if got != want {
			t.Errorf("field %q: got %s, want %s", field, got, want)
		}
	}
}

func TestInferFromJSON_NotJSON(t *testing.T) {
	_, err := InferFromJSON([]byte("not json"))
	if err == nil {
		t.Fatal("expected error for non-JSON input")
	}
}
