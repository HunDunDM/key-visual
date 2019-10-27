package matrix

import (
	"sort"
	"time"
)

// 稀疏key轴
type DiscreteAxis struct {
	StartKey string // 第一条Line的StartKey
	Lines    []*Line
	// StartTime time.Time // EndTime from the previous DiscreteAxis
	EndTime time.Time // 该key轴的time坐标
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

// 计算以指定阈值合并时，生成的DiscreteAxis的Line的数量
func (axis *DiscreteAxis) Effect(threshold uint64) uint {
	// value小于等于threshold且相邻的“线段”可以合并
	// 相邻的且value相等的“线段”可以合并
	var num uint = 0
	isLastLess := false      //标志上一个line的value是否小于threshold
	var lastIndex int64 = -1 //上一个线段的索引
	for _, line := range axis.Lines {
		if line.Useless(threshold) {
			isLastLess = true
		} else { //遇到大于阈值的线段
			if lastIndex == -1 || !line.Value.Equal(axis.Lines[lastIndex].Value) {
				num++
			}
			if isLastLess {
				isLastLess = false
				num++
			}
		}
		lastIndex++
	}
	//处理最后一个线段
	if isLastLess {
		num++
	}
	return num
}

// 以指定阈值合并低于该信息量的线段
func (axis *DiscreteAxis) DeNoise(threshold uint64) {
	newAxis := make([]*Line, 0)
	//value小于threshold且相邻的“线段”可以合并
	// 相邻的且value相等的“线段”可以合并
	isLastLess := false      //标志上一个line的value是否小于threshold
	var lastIndex int64 = -1 //上一个线段的索引
	for _, line := range axis.Lines {
		if line.Useless(threshold) {
			if isLastLess { //若前一个线段也小于阈值，做Merge操作
				newAxis[len(newAxis)-1].Value.Merge(line.Value)
				newAxis[len(newAxis)-1].EndKey = line.EndKey
			} else {
				isLastLess = true
				newAxis = append(newAxis, line)
			}
		} else { //遇到大于阈值的线段
			isLastLess = false
			if lastIndex == -1 || !line.Value.Equal(axis.Lines[lastIndex].Value) {
				newAxis = append(newAxis, line)
			} else { //说明此值与上一个的值相等
				newAxis[len(newAxis)-1].Value.Merge(line.Value)
				newAxis[len(newAxis)-1].EndKey = line.EndKey
			}
		}
		lastIndex++
	}
	axis.Lines = newAxis
}

// 以指定的离散Key序列重采样,
// 只采样line，时间不采样
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
	//srcKeys := axis.GetDiscreteKeys()
	//dstKeys := dst.GetDiscreteKeys()
	lengthSrc := len(axis.Lines)
	lengthDst := len(dst.Lines)
	var DstI int
	var SrcI int
	if axis.StartKey < dst.StartKey {
		DstI = 0
		SrcI = sort.Search(lengthSrc, func(i int) bool {
			return axis.Lines[i].EndKey >= dst.StartKey
		})
	} else {
		DstI = sort.Search(lengthDst, func(i int) bool {
			return axis.StartKey < dst.Lines[i].EndKey
		})
	}

	//源轴中的一段投影到目标轴的上索引范围
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
				//endIndex++
				// 可能后面还存在相等的endKey
				for DstI < lengthDst && dst.Lines[DstI].EndKey == axis.Lines[SrcI].EndKey {
					dst.Lines[DstI].Value.Merge(axis.Lines[SrcI].Value)
					DstI++
					//endIndex++
				}
				startIndex = DstI
			} else {
				startIndex = endIndex
			}
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
