package main

import (
	"reflect"
	"testing"
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

//func TestSingleUnit_Split(t *testing.T) {
//	src := &SingleUnit{
//		Value: 3,
//		Mode: 0,
//	}
//	dst := src.Split(2)
//	check(t, dst.(*MultiUnit), &MultiUnit{
//		Max: MultiValue{
//			10, 20, 30, 40,
//		},
//		Average: MultiValue{
//			50, 100, 150, 200,
//		},
//	})
//	dst = src.Split(5)
//	check(t, dst.(*MultiUnit), &MultiUnit{
//		Max: MultiValue{
//			10, 20, 30, 40,
//		},
//		Average: MultiValue{
//			20, 40, 60, 80,
//		},
//	})
//}
func TestSingleUnit_Merge(t *testing.T) {
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
func TestSingleUnit_Useless(t *testing.T) {
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

func TestSingleUnit_GetThreshold(t *testing.T) {
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
func TestSingleUnit_Clone(t *testing.T) {
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
func TestSingleUnit_Reset(t *testing.T) {
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

func TestSingleUnit_Default(t *testing.T) {
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

func TestSingleUnit_Equal(t *testing.T) {
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

