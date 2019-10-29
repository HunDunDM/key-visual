package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/HunDunDM/key-visual/matrix"
	"sync"
	"time"
)

const defaultpath = "record"

type regionValue struct {
	WrittenBytes uint64 `json:"written_bytes"`
	ReadBytes    uint64 `json:"read_bytes"`
	WrittenKeys  uint64 `json:"written_keys"`
	ReadKeys     uint64 `json:"read_keys"`
}

// 一个统计单元，需要实现matrix.Value
type statUnit struct {
	// 同时计算平均值和最大值
	Max     regionValue `json:"max"`
	Average regionValue `json:"average"`
}

// 返回两个数中的较大值
func max(a uint64, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
func newStatUnit(r *regionInfo) *statUnit {
	rValue := regionValue{
		WrittenBytes: r.WrittenBytes,
		ReadBytes:    r.ReadBytes,
		WrittenKeys:  r.WrittenKeys,
		ReadKeys:     r.ReadKeys,
	}
	return &statUnit{
		Max:     rValue,
		Average: rValue,
	}
}

func (v *statUnit) Split(count int) matrix.Value {
	countU64 := uint64(count)
	res := *v
	res.Average.ReadKeys /= countU64
	res.Average.ReadBytes /= countU64
	res.Average.WrittenKeys /= countU64
	res.Average.WrittenBytes /= countU64
	return &res
}

func (v *statUnit) Merge(other matrix.Value) {
	v2 := other.(*statUnit)
	v.Max.WrittenBytes = max(v.Max.WrittenBytes, v2.Max.WrittenBytes)
	v.Max.WrittenKeys = max(v.Max.WrittenKeys, v2.Max.WrittenKeys)
	v.Max.ReadBytes = max(v.Max.ReadBytes, v2.Max.ReadBytes)
	v.Max.ReadKeys = max(v.Max.ReadKeys, v2.Max.ReadKeys)
	v.Average.WrittenBytes = v.Average.WrittenBytes + v2.Average.WrittenBytes
	v.Average.WrittenKeys = v.Average.WrittenKeys + v2.Average.WrittenKeys
	v.Average.ReadBytes = v.Average.ReadBytes + v2.Average.ReadBytes
	v.Average.ReadKeys = v.Average.ReadKeys + v2.Average.ReadKeys
}

func (v *statUnit) Useless(threshold uint64) bool {
	return max(v.Max.ReadBytes, v.Max.WrittenBytes) < threshold
}

func (v *statUnit) GetThreshold() uint64 {
	return max(v.Max.ReadBytes, v.Max.WrittenBytes)
}

func (v *statUnit) Clone() matrix.Value {
	statUnitClone := *v
	return &statUnitClone
}

func (v *statUnit) Reset() {
	*v = statUnit{}
}

func (v *statUnit) Default() matrix.Value {
	return new(statUnit)
}

func (v *statUnit) Equal(other matrix.Value) bool {
	another := other.(*statUnit)
	return *v == *another
}

type Stat struct {
	sync.RWMutex
	*LeveldbRegion
}

// 将regionInfo转换为key轴并插入Stat中，同时处理分层机制
func (s *Stat) Append(regions []*regionInfo) {
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
	axis := &matrix.DiscreteAxis{
		StartKey: regions[firstIndex].StartKey,
		EndTime:  time.Now(),
	}
	//生成lines
	for _, info := range regions {
		if info == nil {
			continue
		}
		line := &matrix.Line{
			EndKey: info.EndKey,
			Value:  newStatUnit(info),
		}
		axis.Lines = append(axis.Lines, line)
	}
	//对lins的value小于1（即为0）的线段压缩
	axis.DeNoise(1)
	value, _ := json.Marshal(axis)
	nowTime := make([]byte, 8)
	binary.BigEndian.PutUint64(nowTime, uint64(time.Now().Unix()))
	s.Lock()
	defer s.Unlock()
	err := s.Save(string(nowTime), string(value))
	if err != nil {
		fmt.Println(err)
	}
}

func (s *Stat) RangeMatrix(startTime time.Time, endTime time.Time, startKey string, endKey string) *matrix.Matrix {
	//time范围上截取信息
	start := startTime.Unix()
	end := endTime.Unix()
	limit := (end-start)/60 + 1
	s.RLock()
	var startBuf = make([]byte, 8)
	var endBuf = make([]byte, 8)
	binary.BigEndian.PutUint64(startBuf, uint64(start))
	binary.BigEndian.PutUint64(endBuf, uint64(end))
	_, rangeValues, _ := s.LoadRange(string(startBuf), string(endBuf), int(limit))
	s.RUnlock()
	if rangeValues == nil || len(rangeValues) == 0 {
		return nil
	}

	var rangeTimePlane matrix.DiscretePlane
	for _, value := range rangeValues {
		axis := matrix.DiscreteAxis{}
		fmt.Println(value)
		err := json.Unmarshal([]byte(value), &axis)
		if err != nil {
			fmt.Println("unmarshal failed")
			return nil
		}
		rangeTimePlane.Axes = append(rangeTimePlane.Axes, &axis)
	}
	//key范围上截取信息
	for i := 0; i < len(rangeTimePlane.Axes); i++ {
		tempAxis := rangeTimePlane.Axes[i]
		if tempAxis != nil { // 实际应该不会出现nil的情况
			rangeTimePlane.Axes[i] = tempAxis.Range(startKey, endKey)
		}
	}
	newMatrix := rangeTimePlane.Pixel(50, 80)
	return RangeTableID(newMatrix)
}

var globalStat Stat

func init() {
	globalStat.LeveldbRegion, _ = NewLeveldbRegion(defaultpath)
}
