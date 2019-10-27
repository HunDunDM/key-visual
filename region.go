package main

import (
	"encoding/hex"
	"fmt"
)

type regionInfo struct {
	ID           uint64 `json:"id"`
	StartKey     string `json:"start_key"`
	EndKey       string `json:"end_key"`
	WrittenBytes uint64 `json:"written_bytes,omitempty"`
	ReadBytes    uint64 `json:"read_bytes,omitempty"`
	WrittenKeys  uint64 `json:"written_keys,omitempty"`
	ReadKeys     uint64 `json:"read_keys,omitempty"`
}

func (r *regionInfo) String() string {
	return fmt.Sprintf("[%s, %s)", r.StartKey, r.EndKey)
}

func scanRegions() []*regionInfo {
	var key []byte
	var err error
	regions := make([]*regionInfo, 0, 1024)
	for {
		info := regionRequest(key, 1024)
		length := len(info.Regions)
		if length == 0 {
			break
		}
		regions = append(regions, info.Regions...)

		lastEndKey := info.Regions[length-1].EndKey
		if lastEndKey == "" {
			break
		}
		key, err = hex.DecodeString(lastEndKey)
		perr(err)
	}
	return regions
}
