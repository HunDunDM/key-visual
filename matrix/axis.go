package matrix

import (
	"sort"
	"time"
)

// a DiscreteAxis is an abstract of all the regionInfos collection at a certain time
type DiscreteAxis struct {
	StartKey string  `json:"start_key"` // the first line's startKey
	Lines    []*Line `json:"lines"`
	EndTime time.Time `json:"end_time"` // the last line's endTime
}

type DiscreteKeys []string

func (axis *DiscreteAxis) Clone() *DiscreteAxis {
	newAxis := &DiscreteAxis{
		StartKey: axis.StartKey,
		EndTime:  axis.EndTime,
	}
	for i := 0; i < len(axis.Lines); i++ {
		line := &Line{
			EndKey: axis.Lines[i].EndKey,
			Value:  axis.Lines[i].Value.Clone(),
		}
		newAxis.Lines = append(newAxis.Lines, line)
	}
	return newAxis
}

// generate thresholds and sort them from small to big
func (axis *DiscreteAxis) GenerateThresholds() []uint64 {
	// use map to delete duplicated ones
	thresholdsSet := make(map[uint64]struct{}, len(axis.Lines))
	for _, line := range axis.Lines {
		thresholdsSet[line.GetThreshold()] = struct{}{}
	}

	thresholds := make([]uint64, 0, len(thresholdsSet))
	for dif := range thresholdsSet {
		thresholds = append(thresholds, dif)
	}
	sort.Slice(thresholds, func(i, j int) bool { return thresholds[i] < thresholds[j] })
	return thresholds
}

// check if we can merge at the certain threshold
func IsMerge(values []uint64, threshold uint64) bool {
	if len(values) < 2 {
		return true
	}
	var max uint64
	var min uint64
	if values[0] < values[1] {
		min = values[0]
		max = values[1]
	} else {
		min = values[1]
		max = values[0]
	}
	// find the maximum and minimum in values slice
	for i := 2; i < len(values); i++ {
		if values[i] > max {
			max = values[i]
		} else if values[i] < min {
			min = values[i]
		}
	}
	// compare the difference between maximum and minimum with threshold
	if max-min <= threshold {
		return true
	} else {
		return false
	}
}

// calculate the amount of buckets when compressing at a certain threshold
func (axis *DiscreteAxis) Effect(step int, threshold uint64) uint {
	// if 'step' lines' differences between maximum and minimum are all less than threshold, then the axis can be merged
	if step <= 1 {
		return uint(len(axis.Lines))
	}
	i := 0
	values := make([]uint64, 0, step)
	num := 0
	for i < len(axis.Lines) {
		for j := 0; j < step && i+j < len(axis.Lines); j++ {
			values = append(values, axis.Lines[i+j].Value.GetThreshold())
		}
		if IsMerge(values, threshold) {
			// if we can merge, skip 'step' lines
			i += step
		} else {
			// otherwise, the 'window' slides one block to the back
			i++
		}
		// 清空values
		values = make([]uint64, 0, step)
		num++
	}
	return uint(num)
}

// squash axis at certain step and threshold
func (axis *DiscreteAxis) Squash(step int, threshold uint64) {
	// 步长个线段的最大值和最小值的差值小于等于阈值threshold，则可以合并
	if step <= 1 {
		return
	}
	newAxis := make([]*Line, 0)
	i := 0
	values := make([]uint64, 0, step)
	for i < len(axis.Lines) {
		for j := 0; j < step && i+j < len(axis.Lines); j++ {
			values = append(values, axis.Lines[i+j].Value.GetThreshold())
		}
		if IsMerge(values, threshold) {
			newAxis = append(newAxis, axis.Lines[i])
			for j := 1; j < step && i+j < len(axis.Lines); j++ {
				newAxis[len(newAxis)-1].Value.Merge(axis.Lines[i+j].Value)
				newAxis[len(newAxis)-1].EndKey = axis.Lines[i+j].EndKey
			}
			i += step
		} else {
			newAxis = append(newAxis, axis.Lines[i])
			i++
		}
		// 清空values
		values = make([]uint64, 0, step)
	}
	axis.Lines = newAxis
}

// 以指定的离散Key序列重采样,
// 只采样line，时间不采样
// dst的划分至少和axis一样细
func (axis *DiscreteAxis) ReSample(dst *DiscreteAxis) {
	srcKeys := axis.GetDiscreteKeys()
	dstKeys := dst.GetDiscreteKeys()
	lengthSrc := len(srcKeys)
	lengthDst := len(dstKeys)
	startIndex := 0
	endIndex := 0
	for i := 1; i < lengthSrc; i++ {
		// 找到源key数组每一个值在目标key数组中的起始索引和结束索引,以计算它在目标数组中会分裂成几段
		for j := endIndex; j < lengthDst; j++ {
			if dstKeys[j] == srcKeys[i-1] {
				startIndex = j
			}
			if dstKeys[j] == srcKeys[i] {
				endIndex = j
				// 由于keys是递增的所以这里可以直接break
				break
			}
		}
		count := endIndex - startIndex
		if count == 0 {
			continue
		}
		newAxis := axis.Lines[i-1].Split(count)
		for j := startIndex; j < endIndex; j++ {
			dst.Lines[j].Merge(newAxis)
		}
	}
}

// 将axis的值投影到dst
// 传入的dst必须是0值，dst的段数往往比axis少
func (axis *DiscreteAxis) DeProjection(dst *DiscreteAxis) {
	lengthSrc := len(axis.Lines)
	lengthDst := len(dst.Lines)
	var DstI int
	var SrcI int
	// 对DstI和SrcI处理，确保axis和dst的第SrcI和DstI段line有重合
	if axis.StartKey < dst.StartKey {
		DstI = 0
		// axis中寻找第一个EndKey比dst.StartKey大的line
		SrcI = sort.Search(lengthSrc, func(i int) bool {
			return axis.Lines[i].EndKey > dst.StartKey
		})
	} else {
		// 此时axis.StartKey >= dst.StartKey
		// dst中寻找第一个EndKey比axis.StartKey大的line
		DstI = sort.Search(lengthDst, func(i int) bool {
			return dst.Lines[i].EndKey > axis.StartKey
		})
	}

	// 源轴中的一段投影到目标轴dst的上索引范围
	startIndex := DstI
	var endIndex int
	for DstI < lengthDst && SrcI < lengthSrc {
		// 找到axis每一段在dst的投影
		if axis.Lines[SrcI].EndKey <= dst.Lines[DstI].EndKey {
			endIndex = DstI
			//找到投影的范围
			for i := startIndex; i <= endIndex; i++ {
				dst.Lines[i].Value.Merge(axis.Lines[SrcI].Value)
			}
			if axis.Lines[SrcI].EndKey == dst.Lines[DstI].EndKey {
				DstI++
				// 可能后面还存在相等的endKey，实际应该不会出现这种情况
				for DstI < lengthDst && dst.Lines[DstI].EndKey == axis.Lines[SrcI].EndKey {
					dst.Lines[DstI].Value.Merge(axis.Lines[SrcI].Value)
					DstI++
				}

			}
			startIndex = DstI
			SrcI++
		} else {
			DstI++
		}
	}
}

// 获取离散化后的key序列，含StartKey
func (axis *DiscreteAxis) GetDiscreteKeys() DiscreteKeys {
	discreteKeys := make(DiscreteKeys, 0)
	discreteKeys = append(discreteKeys, axis.StartKey)
	for _, key := range axis.Lines {
		discreteKeys = append(discreteKeys, key.EndKey)
	}
	return discreteKeys
}

// 在axis中获取(startKey, endKey]范围的line
func (axis *DiscreteAxis) Range(startKey string, endKey string) *DiscreteAxis {
	newAxis := &DiscreteAxis{
		StartKey: "",
		EndTime:  axis.EndTime,
	}
	if endKey <= axis.StartKey {
		return newAxis
	}
	size := len(axis.Lines)
	startIndex := sort.Search(size, func(i int) bool {
		return axis.Lines[i].EndKey > startKey
	})
	if startIndex == size {
		return newAxis
	}

	endIndex := sort.Search(size, func(i int) bool {
		return axis.Lines[i].EndKey >= endKey
	})
	if endIndex != size {
		endIndex++
	}

	if startIndex == 0 {
		newAxis.StartKey = axis.StartKey
	} else {
		newAxis.StartKey = axis.Lines[startIndex-1].EndKey
	}
	newAxis.Lines = make([]*Line, 0, endIndex-startIndex)
	for i := startIndex; i < endIndex; i++ {
		line := &Line{
			axis.Lines[i].EndKey,
			axis.Lines[i].Value.Clone(),
		}
		newAxis.Lines = append(newAxis.Lines, line)
	}
	return newAxis
}
