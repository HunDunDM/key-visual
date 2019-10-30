package main

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"github.com/HunDunDM/key-visual/matrix"
	"sync"
	"time"
)

const defaultRegionPath = "storage/region"

type regionInfo struct {
	ID           uint64 `json:"id"`
	StartKey     string `json:"start_key"`
	EndKey       string `json:"end_key"`
	WrittenBytes uint64 `json:"written_bytes,omitempty"`
	ReadBytes    uint64 `json:"read_bytes,omitempty"`
	WrittenKeys  uint64 `json:"written_keys,omitempty"`
	ReadKeys     uint64 `json:"read_keys,omitempty"`
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

type regionData struct {
	WrittenBytes uint64 `json:"written_bytes"`
	ReadBytes    uint64 `json:"read_bytes"`
	WrittenKeys  uint64 `json:"written_keys"`
	ReadKeys     uint64 `json:"read_keys"`
}

// a storage unit of region information, which needs to implement the matrix.Value interface
type regionUnit struct {
	// calculate average and maximum simultaneously
	Max     regionData `json:"max"`
	Average regionData `json:"average"`
}

func newRegionUnit(r *regionInfo) regionUnit {
	rValue := regionData{
		WrittenBytes: r.WrittenBytes,
		ReadBytes:    r.ReadBytes,
		WrittenKeys:  r.WrittenKeys,
		ReadKeys:     r.ReadKeys,
	}
	return regionUnit{
		Max:     rValue,
		Average: rValue,
	}
}

func (r regionUnit) Merge(other regionUnit) {
	r.Max.WrittenBytes = max(r.Max.WrittenBytes, other.Max.WrittenBytes)
	r.Max.WrittenKeys = max(r.Max.WrittenKeys, other.Max.WrittenKeys)
	r.Max.ReadBytes = max(r.Max.ReadBytes, other.Max.ReadBytes)
	r.Max.ReadKeys = max(r.Max.ReadKeys, other.Max.ReadKeys)
	r.Average.WrittenBytes = r.Average.WrittenBytes + other.Average.WrittenBytes
	r.Average.WrittenKeys = r.Average.WrittenKeys + other.Average.WrittenKeys
	r.Average.ReadBytes = r.Average.ReadBytes + other.Average.ReadBytes
	r.Average.ReadKeys = r.Average.ReadKeys + other.Average.ReadKeys
}

func (r regionUnit) Useless(threshold uint64) bool {
	return max(r.Max.ReadBytes, r.Max.WrittenBytes) < threshold
}

func (r regionUnit) BuildMultiValue() *MultiUnit {
	max := MultiValue{
		r.Max.WrittenBytes,
		r.Max.ReadBytes,
		r.Max.WrittenKeys,
		r.Max.ReadKeys,
	}
	average := MultiValue{
		r.Average.WrittenBytes,
		r.Average.ReadBytes,
		r.Average.WrittenKeys,
		r.Average.ReadKeys,
	}
	return &MultiUnit{
		max,
		average,
	}
}

// here we define another Line structure different from matrix.Line
// because that one uses a interface and cannot be encoded to json string
type Line struct {
	EndKey     string `json:"end_key"`
	regionUnit `json:"region_unit"`
}

type DiscreteAxis struct {
	StartKey string    `json:"start_key"` // the first line's StartKey
	Lines    []*Line   `json:"lines"`
	EndTime  time.Time `json:"end_time"` // the last line's EndTime
}

// merge lines that have values less than threshold
// which is like eliminate the noise point in a map
func (axis *DiscreteAxis) DeNoise(threshold uint64) {
	newAxis := make([]*Line, 0)
	// a consecutive set of lines which all have values less than threshold can be merged
	// a consecutive set of lines which have values that are very close to each other can also be merged
	isLastLess := false      // indicates whether the last line's value is less than threshold
	var lastIndex int64 = -1 // the last line's index
	for _, line := range axis.Lines {
		if line.Useless(threshold) {
			if isLastLess { // if the prior line's value is also less than threshold, do merge operation
				newAxis[len(newAxis)-1].regionUnit.Merge(line.regionUnit)
				newAxis[len(newAxis)-1].EndKey = line.EndKey
			} else {
				isLastLess = true
				newAxis = append(newAxis, line)
			}
		} else { // when meeting a line which has value bigger than threshold
			isLastLess = false
			if lastIndex == -1 || line.regionUnit != axis.Lines[lastIndex].regionUnit {
				newAxis = append(newAxis, line)
			} else { // means that this value is the same as the prior value
				newAxis[len(newAxis)-1].regionUnit.Merge(line.regionUnit)
				newAxis[len(newAxis)-1].EndKey = line.EndKey
			}
		}
		lastIndex++
	}
	axis.Lines = newAxis
}

// convert the regionInfo into key axis and insert it into Stat
func (r *RegionStore) Append(regions []*regionInfo) {
	if len(regions) == 0 {
		return
	}
	if regions[len(regions)-1].EndKey == "" {
		regions[len(regions)-1].EndKey = "~"
	}
	// find the first regionInfo that is not nil
	firstIndex := 0
	for firstIndex < len(regions) {
		if regions[firstIndex] != nil {
			break
		} else {
			firstIndex++
		}
	}
	if firstIndex == len(regions) {
		return
	}
	// generate DiscreteAxis firstly
	axis := &DiscreteAxis{
		StartKey: regions[firstIndex].StartKey,
		EndTime:  time.Now(),
	}
	// generate lines
	for _, info := range regions {
		if info == nil {
			continue
		}
		line := &Line{
			EndKey:     info.EndKey,
			regionUnit: newRegionUnit(info),
		}
		axis.Lines = append(axis.Lines, line)
	}
	// compress those lines that have values 0
	axis.DeNoise(1)

	value, err := json.Marshal(axis)
	perr(err)
	nowTime := make([]byte, 8)
	binary.BigEndian.PutUint64(nowTime, uint64(time.Now().Unix()))
	r.Lock()
	defer r.Unlock()
	err = r.Save(nowTime, value)
	perr(err)
}

func (r *RegionStore) Range(startTime time.Time, endTime time.Time, separateValue func(r *regionUnit) matrix.Value) *matrix.DiscretePlane {
	// range information in time axis
	start := startTime.Unix()
	end := endTime.Unix()
	var startBuf = make([]byte, 8)
	var endBuf = make([]byte, 8)
	binary.BigEndian.PutUint64(startBuf, uint64(start))
	binary.BigEndian.PutUint64(endBuf, uint64(end))

	r.RLock()
	_, rangeValues, err := r.LoadRange(startBuf, endBuf)
	r.RUnlock()
	perr(err)
	if rangeValues == nil || len(rangeValues) == 0 {
		return nil
	}
	var rangeTimePlane matrix.DiscretePlane
	for _, value := range rangeValues {
		axis := DiscreteAxis{}
		err := json.Unmarshal([]byte(value), &axis)
		perr(err)

		lines := make([]*matrix.Line, len(axis.Lines))
		for i, v := range axis.Lines {
			lines[i] = &matrix.Line{
				EndKey: v.EndKey,
				Value:  separateValue(&v.regionUnit),
			}
		}
		newAxis := matrix.DiscreteAxis{
			StartKey: axis.StartKey,
			Lines:    lines,
			EndTime:  axis.EndTime,
		}
		rangeTimePlane.Axes = append(rangeTimePlane.Axes, &newAxis)
	}
	return &rangeTimePlane
}

type RegionStore struct {
	sync.RWMutex
	*LeveldbStorage
}

var globalRegionStore RegionStore

func init() {
	globalRegionStore.LeveldbStorage, _ = NewLeveldbStorage(defaultRegionPath)
}
