package main

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"
	"github.com/HunDunDM/key-visual/matrix"
)

const defaultRegionPath  = "storage/region"

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

// 一个region信息的存储单元，需要实现matrix.Value
type regionUnit struct {
	// 同时计算平均值和最大值
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
	max := MultiValue {
		r.Max.WrittenBytes,
		r.Max.ReadBytes,
		r.Max.WrittenKeys,
		r.Max.ReadKeys,
	}
	average := MultiValue {
		r.Average.WrittenBytes,
		r.Average.ReadBytes,
		r.Average.WrittenKeys,
		r.Average.ReadKeys,
	}
	return &MultiUnit {
		max,
		average,
	}
}


type Line struct {
	// StartKey string // EndKey from the previous Line
	EndKey   string `json:"end_key"`
	regionUnit `json:"region_unit"`
}

type DiscreteAxis struct {
	StartKey string  `json:"start_key"` // 第一条Line的StartKey
	Lines    []*Line `json:"lines"`
	// StartTime time.Time // EndTime from the previous DiscreteAxis
	EndTime time.Time `json:"end_time"` // 该key轴的time坐标
}

// 以指定阈值合并低于该信息量的线段
func (axis *DiscreteAxis) DeNoise(threshold uint64) {
	newAxis := make([]*Line, 0)
	// value小于threshold且相邻的“线段”可以合并
	// 相邻的且value相等的“线段”可以合并
	isLastLess := false      //标志上一个line的value是否小于threshold
	var lastIndex int64 = -1 //上一个线段的索引
	for _, line := range axis.Lines {
		if line.Useless(threshold) {
			if isLastLess { //若前一个线段也小于阈值，做Merge操作
				newAxis[len(newAxis)-1].regionUnit.Merge(line.regionUnit)
				newAxis[len(newAxis)-1].EndKey = line.EndKey
			} else {
				isLastLess = true
				newAxis = append(newAxis, line)
			}
		} else { //遇到大于阈值的线段
			isLastLess = false
			if lastIndex == -1 || line.regionUnit != axis.Lines[lastIndex].regionUnit {
				newAxis = append(newAxis, line)
			} else { //说明此值与上一个的值相等
				newAxis[len(newAxis)-1].regionUnit.Merge(line.regionUnit)
				newAxis[len(newAxis)-1].EndKey = line.EndKey
			}
		}
		lastIndex++
	}
	axis.Lines = newAxis
}

// 将regionInfo转换为key轴并插入Stat中，同时处理分层机制
func (r *RegionStore) Append(regions []*regionInfo) {
	if len(regions) == 0 {
		return
	}
	if regions[len(regions)-1].EndKey == "" {
		regions[len(regions)-1].EndKey = "~"
	}
	// 寻找第一个不为空指针的regionInfo
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
	//先生成DiscreteAxis
	axis := &DiscreteAxis{
		StartKey: regions[firstIndex].StartKey,
		EndTime:  time.Now(),
	}
	//生成lines
	for _, info := range regions {
		if info == nil {
			continue
		}
		line := &Line{
			EndKey: info.EndKey,
			regionUnit:  newRegionUnit(info),
		}
		axis.Lines = append(axis.Lines, line)
	}
	//对lins的value小于1（即为0）的线段压缩
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

func (r *RegionStore) Range(startTime time.Time, endTime time.Time, separateValue func(r *regionUnit) matrix.Value ) *matrix.DiscretePlane {
	//time范围上截取信息
	start := startTime.Unix()
	end := endTime.Unix()
	var startBuf = make([]byte, 8)
	var endBuf = make([]byte, 8)
	binary.BigEndian.PutUint64(startBuf, uint64(start))
	binary.BigEndian.PutUint64(endBuf, uint64(end))

	r.RLock()
	_, rangeValues := r.LoadRange(startBuf, endBuf)
	r.RUnlock()
	if rangeValues == nil || len(rangeValues) == 0 {
		return nil
	}

	//fmt.Println(start," ", end, "\n");
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


