package apiversions

import "github.com/ritiraj/kafka-go/internal/protocol"

// BuildBody preserves the v3/v4 default for direct callers.
func BuildBody(errorCode int16) []byte {
	return BuildBodyVersion(4, errorCode)
}

func BuildBodyVersion(version, errorCode int16) []byte {
	e := protocol.NewEncoder()
	e.WriteInt16(errorCode)
	apis := protocol.SupportedAPIs()
	if version >= 3 {
		e.WriteUnsignedVarint(uint64(len(apis) + 1))
	} else {
		e.WriteArrayLen(len(apis))
	}
	for _, api := range apis {
		e.WriteInt16(api.Key)
		e.WriteInt16(api.MinVersion)
		e.WriteInt16(api.MaxVersion)
		if version >= 3 {
			e.WriteTagBuffer()
		}
	}
	if version >= 1 {
		e.WriteInt32(0)
	}
	if version >= 3 {
		e.WriteTagBuffer()
	}
	return e.Bytes()
}
