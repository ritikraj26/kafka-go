package storage

import "testing"

func TestValidationDoesNotAllocatePerRecordValues(t *testing.T) {
	records := make([]Record, 10000)
	for i := range records {
		records[i].Value = []byte("x")
	}
	data, err := EncodeRecordBatch(0, records)
	if err != nil {
		t.Fatal(err)
	}
	result := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := ValidateRecordBatch(data); err != nil {
				b.Fatal(err)
			}
		}
	})
	if n := result.AllocedBytesPerOp(); n > 1024 {
		t.Fatalf("validation allocated %d bytes for record descriptors", n)
	}
	batches, err := ValidateRecordBatches(data)
	if err != nil || len(batches) != 1 || batches[0].Values != nil {
		t.Fatal("validation materialized values")
	}
	count, err := CountRecords(data)
	if err != nil || count != 10000 {
		t.Fatalf("record count=%d err=%v", count, err)
	}
}
