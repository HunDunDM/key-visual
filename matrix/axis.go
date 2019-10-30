package matrix

import (
	"sort"
	"time"
)

// a DiscreteAxis is an abstract of all the regionInfos collection at a certain time
type DiscreteAxis struct {
	StartKey string    `json:"start_key"` // the first line's startKey
	Lines    []*Line   `json:"lines"`
	EndTime  time.Time `json:"end_time"` // the last line's endTime
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
		// clear values
		values = make([]uint64, 0, step)
		num++
	}
	return uint(num)
}

// squash axis at certain step and threshold
func (axis *DiscreteAxis) Squash(step int, threshold uint64) {
	// if 'step' lines' differences between maximum and minimum are all less than threshold, then the axis can be merged
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
		// clear values
		values = make([]uint64, 0, step)
	}
	axis.Lines = newAxis
}

// use binary search to find threshold, compress axis so that the amount of buckets can be as close as
// possible to 'm'
func (axis *DiscreteAxis) BinaryCompress(m int) {
	// compress key axis
	if m == 0 {
		return
	}
	if len(axis.Lines) > m {
		// compress the amount of lines of key axis to 'm'
		thresholdSet := make(map[uint64]struct{}, len(axis.Lines))
		// duplicate removal
		for _, line := range axis.Lines {
			thresholdSet[line.GetThreshold()] = struct{}{}
		}

		thresholds := axis.GenerateThresholds()
		// ceil step
		step := len(axis.Lines) / m
		if step*m != len(axis.Lines) {
			step++
		}
		// binary search
		i := sort.Search(len(thresholds), func(i int) bool {
			return axis.Effect(step, thresholds[i]) <= uint(m)
		})
		// choose the closest one
		threshold1 := thresholds[i]
		num1 := axis.Effect(step, threshold1)
		if i > 0 && num1 != uint(m) {
			threshold2 := thresholds[i-1]
			num2 := axis.Effect(step, threshold2)
			if (int(num2) - m) < (m - int(num1)) {
				axis.Squash(step, threshold2)
			} else {
				axis.Squash(step, threshold1)
			}
		} else {
			axis.Squash(step, threshold1)
		}
	}
}

// use the certain discrete key sets to resample
// only at the key-dimension, not at the time-dimension
// the partition of dst should be at least as thin as axis
func (axis *DiscreteAxis) ReSample(dst *DiscreteAxis) {
	srcKeys := axis.GetDiscreteKeys()
	dstKeys := dst.GetDiscreteKeys()
	lengthSrc := len(srcKeys)
	lengthDst := len(dstKeys)
	startIndex := 0
	endIndex := 0
	for i := 1; i < lengthSrc; i++ {
		// find the startIndex and endIndex of every value in the source key array
		// to calculate how many parts it will split into in the destination array
		for j := endIndex; j < lengthDst; j++ {
			if dstKeys[j] == srcKeys[i-1] {
				startIndex = j
			}
			if dstKeys[j] == srcKeys[i] {
				endIndex = j
				// Because keys are increasing, here we can break directly
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

// project the value of axis on dst
// the formal parameter dst is empty
func (axis *DiscreteAxis) DeProjection(dst *DiscreteAxis) {
	lengthSrc := len(axis.Lines)
	lengthDst := len(dst.Lines)
	var DstI int
	var SrcI int
	// process DstI and SrcI so that the SrcI part of axis and the DstI part of dst have overlap
	if axis.StartKey < dst.StartKey {
		DstI = 0
		// find the first line that has EndKey bigger than dst.EndKey
		SrcI = sort.Search(lengthSrc, func(i int) bool {
			return axis.Lines[i].EndKey > dst.StartKey
		})
	} else {
		// at this time, axis.StartKey >= dst.StartKey
		// find the first line that has EndKey bigger than dst.EndKey
		DstI = sort.Search(lengthDst, func(i int) bool {
			return dst.Lines[i].EndKey > axis.StartKey
		})
	}
	// the index scope of a part of source axis's projection on dst axis
	startIndex := DstI
	var endIndex int
	for DstI < lengthDst && SrcI < lengthSrc {
		// find every part of axis's projection on dst
		if axis.Lines[SrcI].EndKey <= dst.Lines[DstI].EndKey {
			endIndex = DstI
			// find the projection's scope
			for i := startIndex; i <= endIndex; i++ {
				dst.Lines[i].Value.Merge(axis.Lines[SrcI].Value)
			}
			if axis.Lines[SrcI].EndKey == dst.Lines[DstI].EndKey {
				DstI++
				// maybe there exists the same key in the behind
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

// get the key sets after discretizatin
func (axis *DiscreteAxis) GetDiscreteKeys() DiscreteKeys {
	discreteKeys := make(DiscreteKeys, 0)
	discreteKeys = append(discreteKeys, axis.StartKey)
	for _, key := range axis.Lines {
		discreteKeys = append(discreteKeys, key.EndKey)
	}
	return discreteKeys
}

// get the line of scope [startKey, endKey) in axis
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
