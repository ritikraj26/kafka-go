package protocol

import "fmt"

type APIVersion struct {
	Key        int16
	MinVersion int16
	MaxVersion int16
}

// SupportedAPIs is the single source for negotiated versions and dispatch.
// Fetch v0 is additionally accepted for the internal follower, but not advertised.
func SupportedAPIs() []APIVersion {
	return []APIVersion{
		{APIKeyProduce, 11, 11},
		{APIKeyFetch, 16, 16},
		{APIKeyListOffsets, 1, 1},
		{APIKeyMetadata, 12, 12},
		{APIKeyOffsetCommit, 2, 2},
		{APIKeyOffsetFetch, 1, 1},
		{APIKeyFindCoordinator, 0, 0},
		{APIKeyJoinGroup, 0, 0},
		{APIKeyHeartbeat, 0, 0},
		{APIKeyLeaveGroup, 0, 0},
		{APIKeySyncGroup, 0, 0},
		{APIKeyApiVersions, 0, 4},
		{APIKeyDescribeTopicPartitions, 0, 0},
	}
}

func SupportsVersion(key, version int16) bool {
	if key == APIKeyFetch && version == 0 {
		return true
	}
	for _, api := range SupportedAPIs() {
		if api.Key == key {
			return version >= api.MinVersion && version <= api.MaxVersion
		}
	}
	return false
}

func FlexibleRequest(key, version int16) bool {
	switch key {
	case APIKeyProduce:
		return version >= 9
	case APIKeyFetch:
		return version >= 12
	case APIKeyMetadata:
		return version >= 9
	case APIKeyApiVersions:
		return version >= 3
	case APIKeyDescribeTopicPartitions:
		return true
	default:
		return false
	}
}

func ResponseHeaderVersion(key, version int16) int16 {
	// ApiVersions deliberately retains the old response header for negotiation.
	if key != APIKeyApiVersions && FlexibleRequest(key, version) {
		return 1
	}
	return 0
}

func requestDecoder(header *RequestHeader, expectedKey int16) (*Decoder, error) {
	if header.GetAPIKey() != expectedKey || !SupportsVersion(expectedKey, header.GetAPIVersion()) {
		return nil, fmt.Errorf("unsupported API key/version %d/%d", header.GetAPIKey(), header.GetAPIVersion())
	}
	limits := DefaultDecodeLimits()
	if header.decodeLimits != nil {
		limits = *header.decodeLimits
	}
	d, err := NewDecoderWithLimits(header.GetBody(), limits)
	if err != nil {
		return nil, err
	}
	header.decoded = d.budget
	clientID, err := d.ReadNullableString()
	if err != nil {
		return nil, fmt.Errorf("request client ID: %w", err)
	}
	header.clientID = clientID
	if FlexibleRequest(expectedKey, header.GetAPIVersion()) {
		if err := d.ReadTaggedFields(nil); err != nil {
			return nil, fmt.Errorf("request header tags: %w", err)
		}
	}
	return d, nil
}

func finishRequest(d *Decoder, flexible bool) error {
	if flexible {
		if err := d.ReadTaggedFields(nil); err != nil {
			return fmt.Errorf("request body tags: %w", err)
		}
	}
	if d.Remaining() != 0 {
		return fmt.Errorf("unexpected trailing request bytes: %d", d.Remaining())
	}
	return nil
}

func checkCompactArray(count uint64, d *Decoder) error {
	if count == 0 || count-1 > uint64(d.Remaining()) {
		return fmt.Errorf("invalid non-null compact array length %d", count)
	}
	return d.ChargeEntries(int64(count - 1))
}
