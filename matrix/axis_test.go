package matrix

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

// define a type which implements Value interface, used by test
type ValueUint64 struct {
	uint64
}

// return the bigger one of two numbers
func max(a uint64, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func (v *ValueUint64) Split(count int) Value {
	res := *v
	return &res
}
func (v *ValueUint64) Merge(other Value) {
	v2 := other.(*ValueUint64)
	v.uint64 = max(v.uint64, v2.uint64)
}
func (v *ValueUint64) Useless(threshold uint64) bool {
	return v.uint64 < threshold
}
func (v *ValueUint64) GetThreshold() uint64 {
	return v.uint64
}

func (v *ValueUint64) Clone() Value {
	cloneValueUint64 := *v
	return &cloneValueUint64
}

func (v *ValueUint64) Reset() {
	*v = ValueUint64{}
}

func (v *ValueUint64) Default() Value {
	return new(ValueUint64)
}

func (v *ValueUint64) Equal(other Value) bool {
	another := other.(*ValueUint64)
	return *v == *another
}

// print DiscreteAxis
func SprintDiscreteAxis(axis *DiscreteAxis) string {
	str := fmt.Sprintf("StartKey: %v\n", axis.StartKey)
	for _, line := range axis.Lines {
		str += fmt.Sprintf("[%v, %v]", line.GetThreshold(), line.EndKey)
	}
	str += fmt.Sprintf("\nEndTime: %v\n", axis.EndTime)
	return str
}

func BuildDiscreteAxis(startKey string, keys []string, values []uint64, endTime time.Time) *DiscreteAxis {
	line := make([]*Line, len(values))
	for i := 0; i < len(values); i++ {
		line[i] = &Line{keys[i], &ValueUint64{values[i]}}
	}
	return &DiscreteAxis{
		StartKey: startKey,
		Lines:    line,
		EndTime:  endTime,
	}
}

func TestClone(t *testing.T) {
	startKey := ""
	uint64List := []uint64{0, 0, 10, 2, 4, 3, 0, 7, 11, 2}
	endKeyList := []string{"a", "b", "d", "e", "h", "i", "k", "l", "t", "z"}
	endTime := time.Now()
	axis := BuildDiscreteAxis(startKey, endKeyList, uint64List, endTime)
	axisClone := axis.Clone()
	// check if the data after clone is the same
	if !reflect.DeepEqual(axisClone, axis) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(axisClone), SprintDiscreteAxis(axis))
	}

	bigUint64 := uint64(100000)
	expectUint64 := axis.Lines[0].GetThreshold()
	axisClone.Lines[0].Merge(&ValueUint64{bigUint64})
	// after change, the data should not be modified
	if reflect.DeepEqual(axis, axisClone) {
		t.Fatalf("expect %v, but got %v", expectUint64, bigUint64)
	}
}

func TestGenerateThresholds(t *testing.T) {
	startKey := ""
	uint64List := []uint64{0, 0, 10, 2, 3, 3, 0, 7, 11, 2}
	endKeyList := []string{"a", "b", "d", "e", "h", "i", "k", "l", "t", "z"}
	endTime := time.Now()
	axis := BuildDiscreteAxis(startKey, endKeyList, uint64List, endTime)

	expect := []uint64{0, 2, 3, 7, 10, 11}
	result := axis.GenerateThresholds()

	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("expect %v, but got %v", expect, result)
	}
}

func TestIsMerge(t *testing.T) {
	values := []uint64{4, 2, 8, 5}

	thresholds := uint64(5)
	expect := false
	result := IsMerge(values, thresholds)
	if !reflect.DeepEqual(expect, result) {
		t.Fatalf("expect %v, but got %v", expect, result)
	}

	thresholds = uint64(6)
	expect = true
	result = IsMerge(values, thresholds)
	if !reflect.DeepEqual(expect, result) {
		t.Fatalf("expect %v, but got %v", expect, result)
	}
}

func TestEffect(t *testing.T) {
	startKey := ""
	uint64List := []uint64{0, 0, 10, 2, 3, 3, 0, 7, 11, 2}
	endKeyList := []string{"a", "b", "d", "e", "h", "i", "k", "l", "t", "z"}
	endTime := time.Now()
	axis := BuildDiscreteAxis(startKey, endKeyList, uint64List, endTime)

	thresholds := axis.GenerateThresholds()
	step := 3
	num := make([]uint, len(thresholds))

	expect := []uint{10, 8, 8, 8, 4, 4}
	for i := 0; i < len(thresholds); i++ {
		num[i] = axis.Effect(step, thresholds[i])
	}

	if !reflect.DeepEqual(num, expect) {
		t.Fatalf("expect %v, but got %v", expect, num)
	}
}

func TestSquash(t *testing.T) {
	startKey := ""
	uint64List := []uint64{4, 0, 10, 2, 3, 3, 0, 7, 11, 5, 1}
	endKeyList := []string{"a", "b", "d", "e", "h", "i", "k", "l", "t", "w", "z"}
	endTime := time.Now()
	axis := BuildDiscreteAxis(startKey, endKeyList, uint64List, endTime)

	// the first test
	threshold1 := uint64(3)
	step1 := 3
	expectUint64List1 := []uint64{4, 0, 10, 3, 0, 7, 11, 5, 1}
	expectEndKeyList1 := []string{"a", "b", "d", "i", "k", "l", "t", "w", "z"}
	expectAxis1 := BuildDiscreteAxis(startKey, expectEndKeyList1, expectUint64List1, endTime)
	axis.Squash(step1, threshold1)
	if !reflect.DeepEqual(axis, expectAxis1) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis1), SprintDiscreteAxis(axis))
	}

	/**********************************************************/
	newAxis := BuildDiscreteAxis(startKey, endKeyList, uint64List, endTime)
	// the second test
	threshold2 := uint64(6)
	step2 := 3
	expectUint64List2 := []uint64{4, 0, 10, 3, 0, 11, 1}
	expectEndKeyList2 := []string{"a", "b", "d", "i", "k", "w", "z"}
	expectAxis2 := BuildDiscreteAxis(startKey, expectEndKeyList2, expectUint64List2, endTime)
	newAxis.Squash(step2, threshold2)
	if !reflect.DeepEqual(newAxis, expectAxis2) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis2), SprintDiscreteAxis(newAxis))
	}
}

func TestBinaryCompress(t *testing.T) {
	startKey := ""
	uint64List := []uint64{4, 0, 10, 2, 3, 3, 0, 7, 11, 5, 1}
	endKeyList := []string{"a", "b", "d", "e", "h", "i", "k", "l", "t", "w", "z"}
	endTime := time.Now()
	axis := BuildDiscreteAxis(startKey, endKeyList, uint64List, endTime)

	expectUint64List := []uint64{10, 3, 0, 11, 1}
	expectEndKeyList := []string{"d", "i", "k", "w", "z"}
	axis.BinaryCompress(5)
	expectAxis := BuildDiscreteAxis(startKey, expectEndKeyList, expectUint64List, endTime)
	if !reflect.DeepEqual(axis, expectAxis) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis), SprintDiscreteAxis(axis))
	}
}

func TestReSample(t *testing.T) {
	startKey := ""
	uint64List := []uint64{0, 0, 10, 2, 4, 3, 0, 7, 11, 2}
	endKeyList := []string{"a", "c", "d", "h", "i", "m", "q", "t", "x", "z"}
	endTime := time.Now()
	axis := BuildDiscreteAxis("\n", endKeyList, uint64List, endTime)

	desKeyList := []string{"a", "b", "c", "d", "f", "h", "i", "l", "m", "o", "p", "q", "t", "x", "z", "zz"}
	desUint64List := []uint64{2, 1, 0, 10, 3, 1, 5, 9, 3, 0, 2, 0, 3, 7, 2, 0}
	expectUint64List := []uint64{2, 1, 0, 10, 3, 2, 5, 9, 3, 0, 2, 0, 7, 11, 2, 0}
	desAxis := BuildDiscreteAxis(startKey, desKeyList, desUint64List, endTime)
	expectAxis := BuildDiscreteAxis(startKey, desKeyList, expectUint64List, endTime)

	axis.ReSample(desAxis)
	if !reflect.DeepEqual(desAxis, expectAxis) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis), SprintDiscreteAxis(desAxis))
	}

	// test the empty axis condition
	axis = BuildDiscreteAxis("\n2", []string{}, []uint64{}, endTime)
	expectAxis = desAxis.Clone()
	axis.ReSample(desAxis)
	if !reflect.DeepEqual(desAxis, expectAxis) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis), SprintDiscreteAxis(desAxis))
	}
}

func TestDeProjection(t *testing.T) {
	startKey := "\n"
	uint64List := []uint64{0, 0, 10, 2, 4, 3, 0, 7, 11, 2}
	endKeyList := []string{"b", "c", "d", "h", "i", "m", "q", "t", "x", "z"}
	endTime := time.Now()
	axis := BuildDiscreteAxis(startKey, endKeyList, uint64List, endTime)

	desStartKey := ""
	desKeyList := []string{"a", "c", "d", "d", "f", "g", "m", "z"}
	desUint64List := []uint64{0, 0, 0, 0, 0, 0, 0, 0}

	expectUint64List := []uint64{0, 0, 10, 10, 2, 2, 4, 11}
	desAxis := BuildDiscreteAxis(desStartKey, desKeyList, desUint64List, endTime)
	expectAxis := BuildDiscreteAxis(desStartKey, desKeyList, expectUint64List, endTime)

	axis.DeProjection(desAxis)
	if !reflect.DeepEqual(desAxis, expectAxis) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis), SprintDiscreteAxis(desAxis))
	}
}

func TestGetDiscreteKeys(t *testing.T) {
	startKey := ""
	uint64List := []uint64{0, 0, 10, 2, 4, 3, 0, 7, 11, 2}
	endKeyList := []string{"a", "c", "d", "h", "i", "m", "q", "t", "x", "z"}
	endTime := time.Now()
	axis := BuildDiscreteAxis(startKey, endKeyList, uint64List, endTime)

	expectKeys := DiscreteKeys{"", "a", "c", "d", "h", "i", "m", "q", "t", "x", "z"}
	keys := axis.GetDiscreteKeys()
	if !reflect.DeepEqual(keys, expectKeys) {
		t.Fatalf("expect %v, but got %v", expectKeys, keys)
	}

	// test the condition that the Lines in DiscreteAxis is empty
	axis.Lines = []*Line{}
	expectKeys = DiscreteKeys{""}
	keys = axis.GetDiscreteKeys()
	if !reflect.DeepEqual(keys, expectKeys) {
		t.Fatalf("expect %v, but got %v", expectKeys, keys)
	}
}

func TestRange(t *testing.T) {
	startKey := ""
	uint64List := []uint64{0, 0, 10, 2, 4, 3, 0, 7, 11, 2}
	endKeyList := []string{"a", "c", "d", "h", "i", "m", "q", "t", "x", "y"}
	endTime := time.Now()
	axis := BuildDiscreteAxis(startKey, endKeyList, uint64List, endTime)

	start := ""
	end := "z"
	expectAxis := axis.Clone()
	rangeAxis := axis.Range(start, end)
	if !reflect.DeepEqual(expectAxis, rangeAxis) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis), SprintDiscreteAxis(rangeAxis))
	}

	start = ""
	end = "\n"
	expectAxis = &DiscreteAxis{
		StartKey: "",
		Lines:    []*Line{{axis.Lines[0].EndKey, axis.Lines[0].Clone()}},
		EndTime:  axis.EndTime,
	}
	rangeAxis = axis.Range(start, end)
	if !reflect.DeepEqual(expectAxis, rangeAxis) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis), SprintDiscreteAxis(rangeAxis))
	}

	start = "b"
	end = "o"
	startIndex := 1
	endIndex := 7
	expectAxis = &DiscreteAxis{
		StartKey: "a",
		Lines:    make([]*Line, 0, endIndex-startIndex),
		EndTime:  axis.EndTime,
	}
	for i := startIndex; i < endIndex; i++ {
		expectAxis.Lines = append(expectAxis.Lines, &Line{axis.Lines[i].EndKey, axis.Lines[i].Clone()})
	}
	rangeAxis = axis.Range(start, end)
	if !reflect.DeepEqual(expectAxis, rangeAxis) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis), SprintDiscreteAxis(rangeAxis))
	}

	start = "\n"
	end = "o"
	startIndex = 0
	endIndex = 7
	expectAxis = &DiscreteAxis{
		StartKey: "",
		Lines:    make([]*Line, 0, endIndex-startIndex),
		EndTime:  axis.EndTime,
	}
	for i := startIndex; i < endIndex; i++ {
		expectAxis.Lines = append(expectAxis.Lines, &Line{axis.Lines[i].EndKey, axis.Lines[i].Clone()})
	}
	rangeAxis = axis.Range(start, end)
	if !reflect.DeepEqual(expectAxis, rangeAxis) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis), SprintDiscreteAxis(rangeAxis))
	}
}
