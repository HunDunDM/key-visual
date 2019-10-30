package main

import "testing"

func check(t *testing.T, src *statUnit, dst *statUnit) {
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
func TestStatUnit_Split(t *testing.T) {
	src := &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			100, 200, 300, 400,
		},
	}
	dst := src.Split(2)
	check(t, dst.(*statUnit), &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			50, 100, 150, 200,
		},
	})
	dst = src.Split(5)
	check(t, dst.(*statUnit), &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			20, 40, 60, 80,
		},
	})
}
func TestStatUnit_Merge(t *testing.T) {
	src := &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			100, 200, 300, 400,
		},
	}
	dst := &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			20, 40, 60, 80,
		},
	}
	src.Merge(dst)
	check(t, src, &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			120, 240, 360, 480,
		},
	})
}
func TestStatUnit_Useless(t *testing.T) {
	src := &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			100, 200, 300, 400,
		},
	}
	src2 := &statUnit{
		Max: regionValue{
			70, 80, 30, 40,
		},
		Average: regionValue{
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

func TestStatUnit_GetThreshold(t *testing.T) {
	src := []*statUnit{
		{
			Max: regionValue{
				10, 20, 30, 40,
			},
			Average: regionValue{
				100, 200, 300, 400,
			},
		},
		{
			Max: regionValue{
				70, 80, 30, 40,
			},
			Average: regionValue{
				100, 200, 300, 400,
			},
		},
		{
			Max: regionValue{
				50, 45, 40, 70,
			},
			Average: regionValue{
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
func TestStatUnit_Clone(t *testing.T) {
	src := &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			100, 200, 300, 400,
		},
	}
	dst := src.Clone()
	check(t, src, dst.(*statUnit))
}
func TestStatUnit_Reset(t *testing.T) {
	src := &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			100, 200, 300, 400,
		},
	}
	src.Reset()
	check(t, src, &statUnit{})
}
func TestStatUnit_Default(t *testing.T) {
	src := &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			100, 200, 300, 400,
		},
	}
	dst := src.Default()
	check(t, dst.(*statUnit), &statUnit{})
}