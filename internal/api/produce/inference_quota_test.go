package produce

import (
	"strings"
	"testing"

	"github.com/ritiraj/kafka-go/internal/testutil"
)

func TestLargeRecordSkipsOptionalInferenceWithoutDroppingWrite(t *testing.T) {
	m := testutil.NewMetadataManager(t)
	topic := testutil.CreateTopic(t, m, "orders", 1)
	value := `{"value":"` + strings.Repeat("x", 1<<20) + `"}`
	response := BuildBody(produceRequest(produceBatch(t, value)), m, 1)
	if code := responseError(t, response); code != 0 {
		t.Fatalf("optional inference limit rejected a valid record: %d", code)
	}
	if topic.Partitions[0].GetLogEndOffset() != 1 {
		t.Fatal("large record was not appended")
	}
	if stats := m.Schemas.Stats(); stats.Jobs != 0 || stats.Schemas != 0 || stats.Bytes != 0 {
		t.Fatalf("oversized inference sample was retained: %+v", stats)
	}
}
