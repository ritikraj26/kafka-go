package protocol

// ConsumerProtocol implements binary encoding/decoding for the Kafka
// consumer group wire format (MemberMetadata and MemberAssignment).
// These use standard (non-compact) Kafka encoding: INT16 strings, INT32 arrays.

// MemberMetadata is the binary payload sent inside JoinGroup Protocols[].Metadata.
type MemberMetadata struct {
	Version  int16
	Topics   []string
	UserData []byte
}

// EncodeMemberMetadata serialises MemberMetadata to the Kafka binary format.
func EncodeMemberMetadata(m *MemberMetadata) []byte {
	e := NewEncoder()
	e.WriteInt16(m.Version)
	e.WriteArrayLen(len(m.Topics))
	for _, t := range m.Topics {
		e.WriteString(t)
	}
	e.WriteBytes(m.UserData)
	return e.Bytes()
}

// DecodeMemberMetadata deserialises MemberMetadata from the Kafka binary format.
func DecodeMemberMetadata(data []byte) (*MemberMetadata, error) {
	d := NewDecoder(data)
	version, err := d.ReadInt16()
	if err != nil {
		return nil, err
	}
	numTopics, err := d.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	topics := make([]string, 0, numTopics)
	for i := int32(0); i < numTopics; i++ {
		t, err := d.ReadString()
		if err != nil {
			return nil, err
		}
		topics = append(topics, t)
	}
	userData, err := d.ReadBytes()
	if err != nil {
		return nil, err
	}
	return &MemberMetadata{Version: version, Topics: topics, UserData: userData}, nil
}

// TopicPartition pairs a topic name with a list of partition indices.
type TopicPartition struct {
	Topic      string
	Partitions []int32
}

// MemberAssignment is the binary payload returned in SyncGroup responses
// and stored per member after the group leader computes an assignment.
type MemberAssignment struct {
	Version         int16
	TopicPartitions []TopicPartition
	UserData        []byte
}

// EncodeMemberAssignment serialises MemberAssignment to the Kafka binary format.
func EncodeMemberAssignment(a *MemberAssignment) []byte {
	e := NewEncoder()
	e.WriteInt16(a.Version)
	e.WriteArrayLen(len(a.TopicPartitions))
	for _, tp := range a.TopicPartitions {
		e.WriteString(tp.Topic)
		e.WriteArrayLen(len(tp.Partitions))
		for _, p := range tp.Partitions {
			e.WriteInt32(p)
		}
	}
	e.WriteBytes(a.UserData)
	return e.Bytes()
}

// DecodeMemberAssignment deserialises MemberAssignment from the Kafka binary format.
func DecodeMemberAssignment(data []byte) (*MemberAssignment, error) {
	d := NewDecoder(data)
	version, err := d.ReadInt16()
	if err != nil {
		return nil, err
	}
	numTopics, err := d.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	tps := make([]TopicPartition, 0, numTopics)
	for i := int32(0); i < numTopics; i++ {
		topic, err := d.ReadString()
		if err != nil {
			return nil, err
		}
		numParts, err := d.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		parts := make([]int32, 0, numParts)
		for j := int32(0); j < numParts; j++ {
			p, err := d.ReadInt32()
			if err != nil {
				return nil, err
			}
			parts = append(parts, p)
		}
		tps = append(tps, TopicPartition{Topic: topic, Partitions: parts})
	}
	userData, err := d.ReadBytes()
	if err != nil {
		return nil, err
	}
	return &MemberAssignment{Version: version, TopicPartitions: tps, UserData: userData}, nil
}
