package matrix

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func buildTime(min int) time.Time {
	str := strconv.Itoa(min)
	str += "m"
	dur, _ := time.ParseDuration(str)
	time := time.Now()
	return time.Add(-dur)
}

func BuildDiscretePlane(times []int, keys [][]string, values [][]uint64) *DiscretePlane {
	plane := &DiscretePlane{
		StartTime: buildTime(times[0]),
		Axes:      make([]*DiscreteAxis, len(times)-1),
	}

	for i := 0; i < len(keys); i++ {
		plane.Axes[i] = BuildDiscreteAxis(keys[i][0], keys[i][1:], values[i], buildTime(times[i+1]))
	}
	return plane
}

func buildDiscreteMinutes(times []time.Time) []int {
	minutes := make([]int, len(times))
	for i := 0; i < len(times); i++ {
		minutes[i] = times[i].Minute()
	}
	return minutes
}

// 打印Matrix
func SprintMatrix(matrix *Matrix) string {
	str := fmt.Sprintf("Keys: ")
	for i := 0; i < len(matrix.Keys); i++ {
		str += fmt.Sprintf("%v ", matrix.Keys[i])
	}
	str += "\n"
	str += fmt.Sprintf("Times: ")
	for i := 0; i < len(matrix.Times); i++ {
		str += fmt.Sprintf("%v ", matrix.Times[i].Minute())
	}
	str += "\n"
	for i := 0; i < len(matrix.Data); i++ {
		for j := 0; j < len(matrix.Data[i]); j++ {
			str += fmt.Sprintf("%v ", matrix.Data[i][j].GetThreshold())
		}
		str += "\n"
	}
	return str
}

func TestGetDiscreteTimes(t *testing.T) {
	times := []int{0, 1, 2, 3, 7, 11, 25}
	keys := [][]string{
		{""}, {""}, {""}, {""}, {""}, {""},
	}
	values := make([][]uint64, len(keys))
	plane := BuildDiscretePlane(times, keys, values)

	expectTimes := make([]time.Time, len(times))
	expectTimes[0] = plane.StartTime
	for i := 1; i < len(times); i++ {
		expectTimes[i] = plane.Axes[i-1].EndTime
	}
	expectMinutes := buildDiscreteMinutes(expectTimes)
	discreteTimes := plane.GetDiscreteTimes()
	discreteMinutes := buildDiscreteMinutes(discreteTimes)
	if !reflect.DeepEqual(discreteMinutes, expectMinutes) {
		t.Fatalf("expect %v, but got %v", expectMinutes, discreteMinutes)
	}

	//测试DiscretePlane中Axes为空的情况
	times = []int{0}
	keys = [][]string{}
	values = make([][]uint64, len(keys))
	plane = BuildDiscretePlane(times, keys, values)
	expectTimes = make([]time.Time, len(times))
	expectTimes[0] = plane.StartTime
	expectMinutes = buildDiscreteMinutes(expectTimes)
	discreteTimes = plane.GetDiscreteTimes()
	discreteMinutes = buildDiscreteMinutes(discreteTimes)
	if !reflect.DeepEqual(discreteMinutes, expectMinutes) {
		t.Fatalf("expect %v, but got %v", expectMinutes, discreteMinutes)
	}
}

func TestCompact(t *testing.T) {
	times := []int{20, 15, 10, 5, 0}
	keys := [][]string{
		{"z"},
		{"", "b", "f", "h", "i"},
		{"a", "d", "i", "n", "q"},
		{"", "e", "i", "k", "n"},
	}
	values := [][]uint64{
		{},
		{1, 5, 4, 10},
		{5, 0, 1, 6},
		{0, 3, 7, 9},
	}
	plane := BuildDiscretePlane(times, keys, values)
	resultAxis, resultStartTime := plane.Compact()

	endTime := plane.Axes[len(plane.Axes)-1].EndTime
	startKey := ""
	uint64List := []uint64{1, 5, 5, 5, 5, 4, 10, 7, 9, 6}
	endKeyList := []string{"a", "b", "d", "e", "f", "h", "i", "k", "n", "q"}
	expectAxis := BuildDiscreteAxis(startKey, endKeyList, uint64List, endTime)
	expectStartTime := plane.StartTime

	if !reflect.DeepEqual(resultStartTime, expectStartTime) {
		t.Fatalf("expect %v, but got %v", expectStartTime, resultStartTime)
	}
	if !reflect.DeepEqual(expectAxis, resultAxis) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis), SprintDiscreteAxis(resultAxis))
	}
}

func TestPixel(t *testing.T) {
	times := []int{20, 15, 10, 5, 0}
	keys := [][]string{
		{"b", "c", "e", "l", "m", "o"},
		{"", "b", "f", "h", "i", "k"},
		{"a", "d", "i", "n", "q", "r"},
		{"", "e", "i", "k", "n", "o"},
	}
	values := [][]uint64{
		{3, 0, 6, 0, 9},
		{1, 5, 4, 10, 7},
		{5, 0, 1, 6, 4},
		{0, 3, 7, 9, 5},
	}
	plane := BuildDiscretePlane(times, keys, values)
	//{"b", "c", "e", "l", "m", "o"},
	//{"",  "b", "f", "h", "i", "k"},
	//{"",  "b", "c", "e", "f", "h", "i", "k", "l", "m", "o", }
	//{       1,   5,   5,   6,   6,  10,   7,   6,   0,   9, }

	//{"a", "d", "i", "n", "q", "r"},
	//{"",  "e", "i", "k", "n", "o"},
	//{"",  "a", "d", "e", "i", "k", "n", "o", "q", "r",}
	//{       0,   5,   0,   3,   7,   9,   6,   6,   4,}

	//基轴DeNoise 前
	//{"", "a", "b", "c", "d", "e", "f", "h", "i", "k", "l", "m", "n", "o", "q", "r",}
	//{      1,   5,   5,   5,   5,   6,   6,  10,   7,   9,   9,   9,   9,  6,   4, }

	//threshold := 6
	// 基轴DeNiose 后
	//{"", "c", "f", "k", "n", "o", "r",}
	//{      5,  6,  10,   9,   9,   6,}

	//1
	//{"",  "b", "c", "e", "f", "h", "i", "k", "l", "m", "o", }
	//{       1,   5,   5,   6,   6,  10,   7,   6,   0,   9, }
	//{      5,  6,   10,   9,   9,   0,}
	//2
	//{"",  "a", "d", "e", "i", "k", "n", "o", "q", "r",}
	//{       0,   5,   0,   3,   7,   9,   6,   6,   4,}
	//{      5,   5,   7,   9,   6,   6,}
	timeN := DiscreteTimes{plane.StartTime, plane.Axes[1].EndTime, plane.Axes[3].EndTime}
	keyM := DiscreteKeys{"", "c", "f", "k", "n", "o", "r"}

	uint64NM := [][]uint64{
		{5, 6, 10, 9, 9, 0},
		{5, 5, 7, 9, 6, 6},
	}

	expectMatrix := &Matrix{
		Times: timeN,
		Data:  make([][]Value, len(uint64NM)),
		Keys:  keyM,
	}
	for i := 0; i < len(uint64NM); i++ {
		expectMatrix.Data[i] = make([]Value, len(uint64NM[i]))
		for j := 0; j < len(uint64NM[i]); j++ {
			expectMatrix.Data[i][j] = &ValueUint64{uint64NM[i][j]}
		}
	}

	n := 2
	m := 7
	matrix := plane.Pixel(n, m)
	if !reflect.DeepEqual(expectMatrix, matrix) {
		t.Fatalf("expect: %v\nbut got: %v", SprintMatrix(expectMatrix), SprintMatrix(matrix))
	}
}
