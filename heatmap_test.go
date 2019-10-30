package main

import (
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestMax(t *testing.T) {
	a := uint64(1)
	b := uint64(2)
	result := Max(a,b)
	expect := b
	if !reflect.DeepEqual(expect, result) {
		t.Fatalf("expect %v, but got %v", expect, result)
	}

	a = uint64(3)
	result = Max(a,b)
	expect = a
	if !reflect.DeepEqual(expect, result) {
		t.Fatalf("expect %v, but got %v", expect, result)
	}
}

func check(t *testing.T, src *MultiUnit, dst *MultiUnit) {
	if src.Average.WrittenBytes != dst.Average.WrittenBytes {
		t.Fatalf("Average WrittenBytes expect %d but get %d", src.Average.WrittenBytes, dst.Average.WrittenBytes)
	}
	if src.Average.WrittenKeys != dst.Average.WrittenKeys {
		t.Fatalf("Average WrittenKeys expect %d but get %d", src.Average.WrittenKeys, dst.Average.WrittenKeys)
	}
	if src.Average.ReadBytes != dst.Average.ReadBytes {
		t.Fatalf("Average ReadBytes expect %d but get %d", src.Average.ReadBytes, dst.Average.ReadBytes)
	}
	if src.Average.ReadKeys != dst.Average.ReadKeys {
		t.Fatalf("Average ReadKeys expect %d but get %d", src.Average.ReadKeys, dst.Average.ReadKeys)
	}
	if src.Max.WrittenBytes != dst.Max.WrittenBytes {
		t.Fatalf("Max WrittenBytes expect %d but get %d", src.Max.WrittenBytes, dst.Max.WrittenBytes)
	}
	if src.Max.WrittenKeys != dst.Max.WrittenKeys {
		t.Fatalf("Max WrittenKeys expect %d but get %d", src.Max.WrittenKeys, dst.Max.WrittenKeys)
	}
	if src.Max.ReadBytes != dst.Max.ReadBytes {
		t.Fatalf("Max ReadBytes expect %d but get %d", src.Max.ReadBytes, dst.Max.ReadBytes)
	}
	if src.Max.ReadKeys != dst.Max.ReadKeys {
		t.Fatalf("Max ReadKeys expect %d but get %d", src.Max.ReadKeys, dst.Max.ReadKeys)
	}
}

func TestMultiUnit_Split(t *testing.T) {
	src := &MultiUnit{
		Max: MultiValue{
			10, 20, 30, 40,
		},
		Average: MultiValue{
			100, 200, 300, 400,
		},
	}
	dst := src.Split(2)
	check(t, dst.(*MultiUnit), &MultiUnit{
		Max: MultiValue{
			10, 20, 30, 40,
		},
		Average: MultiValue{
			50, 100, 150, 200,
		},
	})
	dst = src.Split(5)
	check(t, dst.(*MultiUnit), &MultiUnit{
		Max: MultiValue{
			10, 20, 30, 40,
		},
		Average: MultiValue{
			20, 40, 60, 80,
		},
	})
}
func TestMultiUnit_Merge(t *testing.T) {
	src := &MultiUnit{
		Max: MultiValue{
			10, 20, 30, 40,
		},
		Average: MultiValue{
			100, 200, 300, 400,
		},
	}
	dst := &MultiUnit{
		Max: MultiValue{
			10, 20, 30, 40,
		},
		Average: MultiValue{
			20, 40, 60, 80,
		},
	}
	src.Merge(dst)
	check(t, src, &MultiUnit{
		Max: MultiValue{
			10, 20, 30, 40,
		},
		Average: MultiValue{
			120, 240, 360, 480,
		},
	})
}
func TestMultiUnit_Useless(t *testing.T) {
	src := &MultiUnit{
		Max: MultiValue{
			10, 20, 30, 40,
		},
		Average: MultiValue{
			100, 200, 300, 400,
		},
	}
	src2 := &MultiUnit{
		Max: MultiValue{
			70, 80, 30, 40,
		},
		Average: MultiValue{
			100, 200, 300, 400,
		},
	}
	threshold := uint64(30)
	check := func(src bool, dst bool) {
		if src != dst {
			t.Fatalf("useless() result not the same\n")
		}
	}
	check(src.Useless(threshold), true)
	check(src2.Useless(threshold), false)
}

func TestMultiUnit_GetThreshold(t *testing.T) {
	src := []*MultiUnit{
		{
			Max: MultiValue{
				10, 20, 30, 40,
			},
			Average: MultiValue{
				100, 200, 300, 400,
			},
		},
		{
			Max: MultiValue{
				70, 80, 30, 40,
			},
			Average: MultiValue{
				100, 200, 300, 400,
			},
		},
		{
			Max: MultiValue{
				50, 45, 40, 70,
			},
			Average: MultiValue{
				100, 200, 300, 400,
			},
		},
	}
	var threshold uint64
	for _, s := range src {
		threshold = s.GetThreshold()
	}
	check := func(src uint64, dst uint64) {
		if src != dst {
			t.Fatalf("threshold not the same\n")
		}
	}
	check(threshold, 50)
}
func TestMultiUnit_Clone(t *testing.T) {
	src := &MultiUnit{
		Max: MultiValue{
			10, 20, 30, 40,
		},
		Average: MultiValue{
			100, 200, 300, 400,
		},
	}
	dst := src.Clone()
	check(t, src, dst.(*MultiUnit))
}
func TestMultiUnit_Reset(t *testing.T) {
	src := &MultiUnit{
		Max: MultiValue{
			10, 20, 30, 40,
		},
		Average: MultiValue{
			100, 200, 300, 400,
		},
	}
	src.Reset()
	check(t, src, &MultiUnit{})
}

func TestMultiUnit_Default(t *testing.T) {
	src := &MultiUnit{
		Max: MultiValue{
			10, 20, 30, 40,
		},
		Average: MultiValue{
			100, 200, 300, 400,
		},
	}
	dst := src.Default()
	check(t, dst.(*MultiUnit), &MultiUnit{})
}
func TestMultiUnit_Equal(t *testing.T) {
	src := &MultiUnit{
		Max: MultiValue{
			10, 20, 30, 40,
		},
		Average: MultiValue{
			100, 200, 300, 400,
		},
	}
	dst := src.Clone()
	check(t, dst.(*MultiUnit), src)
}

/*********************************************************************************************/
/* test SingleUnit */
/*********************************************************************************************/

func TestSingleUnit_Split(t *testing.T) {
	src := &SingleUnit{
		Value: 3,
		Mode: 0,
	}
	dst := src.Split(2)
	result := dst.(*SingleUnit)
	expect := &SingleUnit{
		Value: 3,
		Mode: 0,
	}
	if !reflect.DeepEqual(expect, result) {
		t.Fatalf("expect %v, but got %v", expect, result)
	}

	src.Mode = 1
	dst = src.Split(2)
	result = dst.(*SingleUnit)
	expect = &SingleUnit{
		Value: 1,
		Mode: 1,
	}
	if !reflect.DeepEqual(expect, result) {
		t.Fatalf("expect %v, but got %v", expect, result)
	}
}

func TestSingleUnit_Merge(t *testing.T) {
	src := &SingleUnit{
		Value: 3,
		Mode: 0,
	}
	dst := &SingleUnit{
		Value: 4,
		Mode: 0,
	}
	src.Merge(dst)

	expect := &SingleUnit{
		Value: 4,
		Mode: 0,
	}
	if !reflect.DeepEqual(expect, src) {
		t.Fatalf("expect %v, but got %v", expect, src)
	}

	src = &SingleUnit{
		Value: 3,
		Mode: 1,
	}
	dst = &SingleUnit{
		Value: 4,
		Mode: 1,
	}
	src.Merge(dst)

	expect = &SingleUnit{
		Value: 7,
		Mode: 1,
	}
	if !reflect.DeepEqual(expect, src) {
		t.Fatalf("expect %v, but got %v", expect, src)
	}
}

func TestSingleUnit_Useless(t *testing.T) {
	src := &SingleUnit{
		Value: 3,
	}

	result := src.Useless(4)
	expect := true
	if !reflect.DeepEqual(expect, result) {
		t.Fatalf("expect %v, but got %v", expect, result)
	}

	result = src.Useless(3)
	expect = false
	if !reflect.DeepEqual(expect, result) {
		t.Fatalf("expect %v, but got %v", expect, result)
	}
}

func TestSingleUnit_GetThreshold(t *testing.T) {
	src := &SingleUnit{
		Value: 3,
	}

	result := src.GetThreshold()
	expect := uint64(3)
	if !reflect.DeepEqual(expect, result) {
		t.Fatalf("expect %v, but got %v", expect, result)
	}
}

func TestSingleUnit_Clone(t *testing.T) {
	src := &SingleUnit{
		Value: 3,
		Mode: 1,
	}

	dst := src.Clone()
	result := dst.(*SingleUnit)
	if !reflect.DeepEqual(src, result) {
		t.Fatalf("expect %v, but got %v", src, result)
	}

	expect := &SingleUnit{
		Value: 3,
		Mode: 1,
	}
	src.Value = 10
	if reflect.DeepEqual(src, result) {
		t.Fatalf("expect %v, but got %v", expect, result)
	}
}

func TestSingleUnit_Reset(t *testing.T) {
	src := &SingleUnit {
		Value: 3,
		Mode: 1,
	}
	src.Reset()
	expect := &SingleUnit {
		Value: 0,
		Mode: 1,
	}
	if !reflect.DeepEqual(expect, src) {
		t.Fatalf("expect %v, but got %v", expect, src)
	}
}

func TestSingleUnit_Default(t *testing.T) {
	src := &SingleUnit {
		Value: 3,
		Mode: 1,
	}
	result := src.Default()
	expect := &SingleUnit {
		Value: 0,
		Mode: 1,
	}
	if !reflect.DeepEqual(expect, result) {
		t.Fatalf("expect %v, but got %v", expect, result)
	}

	expect = &SingleUnit {
		Value: 3,
		Mode: 1,
	}
	if !reflect.DeepEqual(expect, src) {
		t.Fatalf("expect %v, but got %v", expect, src)
	}
}

func TestSingleUnit_Equal(t *testing.T) {
	src := &SingleUnit {
		Value: 3,
		Mode: 1,
	}
	dst := &SingleUnit {
		Value: 3,
		Mode: 1,
	}
	result := src.Equal(dst)
	expect := true
	if !reflect.DeepEqual(expect, result) {
		t.Fatalf("expect %v, but got %v", expect, result)
	}

	dst.Value = 10
	result = src.Equal(dst)
	expect = false
	if !reflect.DeepEqual(expect, result) {
		t.Fatalf("expect %v, but got %v", expect, result)
	}
}

func buildTime(min int) time.Time {
	str := strconv.Itoa(min)
	str += "m"
	dur, _ := time.ParseDuration(str)
	time := time.Now()
	return time.Add(-dur)
}

func TestChangeIntoHeatmap(t *testing.T) {
	matrix := &matrix.Matrix {
		Data: [][]matrix.Value {
			[]*SingleUnit {
				&SingleUnit{
					1,0,
				},
				&SingleUnit{
					2,0,
				},
			},
			[]*SingleUnit {
				&SingleUnit{
					3,0,
				},
				&SingleUnit{
					4,0,
				},
			},
		},
		Keys: matrix.DiscreteKeys {
			"", "a", "b",
		},
		Times: matrix.DiscreteTimes {
			buildTime(-3),buildTime(-1),buildTime(0),
		},
	}

	expect := &Heatmap{
		Data:[][]interface{
			[]interface{1,2,},
			[]interface{1,2,},
		},
	}
}

