package network

import (
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestIndependentCodecDataResponses(t *testing.T) {
	address, m, _ := interopBroker(t)
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	produce := kmsg.NewPtrProduceRequest()
	produce.Acks = 1
	produce.Topics = []kmsg.ProduceRequestTopic{{Topic: "events", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0}}}}
	produced := kmsg.NewPtrProduceResponse()
	wireInterop(t, conn, produce, produced, 11)
	if len(produced.Topics) != 1 || len(produced.Topics[0].Partitions) != 1 ||
		produced.Topics[0].Partitions[0].ErrorCode != 0 || produced.Topics[0].Partitions[0].LogAppendTime != -1 {
		t.Fatalf("invalid produce response: %+v", produced)
	}

	fetch := kmsg.NewPtrFetchRequest()
	fetch.MaxWaitMillis = 0
	fetch.MinBytes = 0
	fetch.MaxBytes = 1024
	part := kmsg.NewFetchRequestTopicPartition()
	part.Partition = 0
	part.PartitionMaxBytes = 1024
	fetch.Topics = []kmsg.FetchRequestTopic{{TopicID: m.GetTopic("events").ID, Partitions: []kmsg.FetchRequestTopicPartition{part}}}
	fetched := kmsg.NewPtrFetchResponse()
	wireInterop(t, conn, fetch, fetched, 16)
	if len(fetched.Topics) != 1 || len(fetched.Topics[0].Partitions) != 1 ||
		fetched.Topics[0].Partitions[0].ErrorCode != 0 || fetched.Topics[0].Partitions[0].HighWatermark != 0 {
		t.Fatalf("invalid fetch response: %+v", fetched)
	}

	list := kmsg.NewPtrListOffsetsRequest()
	list.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: "events", Partitions: []kmsg.ListOffsetsRequestTopicPartition{{Partition: 0, Timestamp: -2}}}}
	offsets := kmsg.NewPtrListOffsetsResponse()
	wireInterop(t, conn, list, offsets, 1)
	if len(offsets.Topics) != 1 || len(offsets.Topics[0].Partitions) != 1 ||
		offsets.Topics[0].Partitions[0].ErrorCode != 0 || offsets.Topics[0].Partitions[0].Offset != 0 ||
		offsets.Topics[0].Partitions[0].Timestamp != -1 {
		t.Fatalf("invalid earliest-offset response: %+v", offsets)
	}
	list.Topics[0].Partitions[0].Timestamp = 1234
	wireInterop(t, conn, list, offsets, 1)
	if offsets.Topics[0].Partitions[0].ErrorCode != 43 || offsets.Topics[0].Partitions[0].Offset != -1 {
		t.Fatal("unsupported timestamp lookup returned a success-shaped latest offset")
	}
}
