package main

import (
	"sync"
	"time"

	"github.com/HunDunDM/key-visual/matrix"
)

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

type layerStat struct {
	startTime time.Time // 当前第一条数据的StartTime
	// 循环数组
	ring  []*matrix.DiscreteAxis // 最后一层时为可增长数组，否则为定长循环数组
	head  int                    // 循环数组头指针
	tail  int                    // 循环数组尾指针
	empty bool                   // 当前数组是否为空
	len   int                    // 循环数组最大容量。最后一层时，忽略该值
	// 分层机制
	compactRatio  int        // 将compactRatio个数据压缩为一个，并添加到下一层中。该值必须比循环队列的总容量小
	nextLayerStat *layerStat // nextLayerStat为nil，表示为最后一层，永不丢弃数据
}

// 新建一个layerStat
func newLayerStat(ratio int, len int) *layerStat {
	if ratio == 0 || len == 0 {
		return &layerStat{
			startTime:     time.Now(),
			ring:          make([]*matrix.DiscreteAxis, 0),
			head:          0,
			tail:          0,
			empty:         true,
			len:           0,
			compactRatio:  0,
			nextLayerStat: nil,
		}
	}

	return &layerStat{
		startTime:     time.Now(),
		ring:          make([]*matrix.DiscreteAxis, len, len),
		head:          0,
		tail:          0,
		empty:         true,
		len:           len,
		compactRatio:  ratio,
		nextLayerStat: nil,
	}
}

// 将一条key轴加入layerStat中
func (s *layerStat) Append(axis *matrix.DiscreteAxis) {
	if s.nextLayerStat == nil {
		// 处理不限制容量的情况（最后一层）
		s.ring = append(s.ring, axis)
		s.tail++
		s.empty = false
		return
	}

	if s.head == s.tail && !s.empty {
		// 需要压缩数据以腾出空间
		plane := new(matrix.DiscretePlane)
		plane.StartTime = s.startTime
		plane.Axes = make([]*matrix.DiscreteAxis, s.compactRatio, s.compactRatio)
		for i := 0; i < s.compactRatio; i++ {
			plane.Axes[i] = s.ring[s.head]
			s.head = (s.head + 1) % s.len
		}
		compactAxis, _ := plane.Compact()
		s.startTime = compactAxis.EndTime
		s.nextLayerStat.Append(compactAxis)
	}

	s.ring[s.tail] = axis
	s.empty = false
	s.tail = (s.tail + 1) % s.len
}

// 二分查找一个时间在layerStat的哪一条key轴上,寻找比他晚的最近的那个值
func (s *layerStat) Search(t time.Time) (int, bool) {
	if s.empty {
		return -1, false
	}
	var l, r, end, size int
	if s.nextLayerStat == nil {
		size = len(s.ring)
		l, r, end = 0, size, size
	} else {
		l, r, size = s.head, s.tail, s.len
		if r <= l {
			r += size
		}
		end = r
	}
	for l < r {
		m := (l + r) / 2
		if s.ring[m%size].EndTime.Before(t) {
			l = m + 1
		} else {
			r = m
		}
	}
	if l == end {
		return (end - 1) % size, false
	} else {
		return l % size, true
	}
}

// 给定起始时间和结束时间，求一个key轴的集合即一个平面
func (s *layerStat) Range(startTime time.Time, endTime time.Time) *matrix.DiscretePlane {
	startIndex, ok := s.Search(startTime)
	if !ok {
		if s.nextLayerStat != nil {
			return s.nextLayerStat.Range(startTime, endTime)
		}
		return nil
	}
	endIndex, _ := s.Search(endTime)
	endIndex++
	// 生成plane
	plane := new(matrix.DiscretePlane)
	plane.Axes = make([]*matrix.DiscreteAxis, 0)
	if startIndex == s.head {
		plane.StartTime = s.startTime
	} else if startIndex > 0 {
		plane.StartTime = s.ring[startIndex-1].EndTime
	} else {
		plane.StartTime = s.ring[s.len-1].EndTime
	}
	if endIndex > startIndex {
		plane.Axes = append(plane.Axes, s.ring[startIndex:endIndex]...)
	} else {
		plane.Axes = append(plane.Axes, s.ring[startIndex:s.len]...)
		plane.Axes = append(plane.Axes, s.ring[0:endIndex]...)
	}
	if s.nextLayerStat != nil {
		nextPlane := s.nextLayerStat.Range(startTime, endTime)
		if nextPlane != nil {
			nextPlane.Axes = append(nextPlane.Axes, plane.Axes...)
			return nextPlane
		}
	}
	return plane
}

type stat struct {
	layers []*layerStat
}

type Stat struct {
	sync.RWMutex
	*stat
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
	s.Lock()
	defer s.Unlock()
	s.stat.layers[0].Append(axis)
}

func (s *Stat) RangeMatrix(startTime time.Time, endTime time.Time, startKey string, endKey string) *matrix.Matrix {
	//time范围上截取信息
	s.RLock()
	rangeTimePlane := s.stat.layers[0].Range(startTime, endTime)
	s.RUnlock()
	if rangeTimePlane == nil {
		return nil
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

type layerConfig struct {
	Len   int
	Ratio int
}

var debugLayersConfig = []layerConfig{
	{Len: 15, Ratio: 5},      // 15分钟内，数据单位：分钟
	{Len: 60 / 5, Ratio: 12}, // 约一小时内，数据单位：5分钟
	{Len: 0, Ratio: 0},       // 约一小时后，数据单位：小时
}

var productLayersConfig = []layerConfig{
	{Len: 60 * 12, Ratio: 10},         // 12小时内，数据单位：分钟
	{Len: 60 / 10 * 24 * 3, Ratio: 6}, // 约3天内，数据单位：10分钟
	{Len: 24 * 15, Ratio: 24},         // 约15天内，数据单位：小时
	{Len: 0, Ratio: 0},                // 约15天以前，数据单位：天
}

func init() {
	globalStat.stat = new(stat)
	globalStat.stat.layers = make([]*layerStat, 0)
	for i, config := range productLayersConfig {
		globalStat.stat.layers = append(globalStat.stat.layers, newLayerStat(config.Ratio, config.Len))
		if i > 0 {
			globalStat.stat.layers[i-1].nextLayerStat = globalStat.stat.layers[i]
		}
	}
}
