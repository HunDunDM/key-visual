package main

import (
	"encoding/hex"
	"github.com/HunDunDM/key-visual/matrix"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"testing"
	"time"
)

func encodeTablePrefix(tableID int64) string {
	key := tablecodec.EncodeTablePrefix(tableID)
	raw := codec.EncodeBytes([]byte(nil), key)
	return hex.EncodeToString(raw)
}

func encodeTableIndexPrefix(tableID int64, indexID int64) string {
	key := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
	raw := codec.EncodeBytes([]byte(nil), key)
	return hex.EncodeToString(raw)
}
func newRegionInfo(start string, end string, writtenBytes uint64, writtenKeys uint64, readBytes uint64, readKeys uint64) *regionInfo {

	return &regionInfo{
		StartKey:     start,
		EndKey:       end,
		WrittenBytes: writtenBytes,
		WrittenKeys:  writtenKeys,
		ReadBytes:    readBytes,
		ReadKeys:     readKeys,
	}
}
func newDiscreteAxis(regions []*regionInfo) *matrix.DiscreteAxis {
	axis := &matrix.DiscreteAxis{
		StartKey: regions[0].StartKey,
		EndTime:  time.Now(),
	}
	//生成lines
	for _, info := range regions {
		line := &matrix.Line{
			EndKey: info.EndKey,
			Value:  newStatUnit(info),
		}
		axis.Lines = append(axis.Lines, line)
	}
	//对lins的value小于1（即为0）的线段压缩
	axis.DeNoise(1)
	return axis
}
func TestStat_Append(t *testing.T) {
	var testStat Stat
	testStat.stat = new(stat)
	testStat.stat.layers = make([]*layerStat, 0)
	for i, config := range debugLayersConfig {
		testStat.stat.layers = append(testStat.stat.layers, newLayerStat(config.Len, config.Ratio))
		if i > 0 {
			testStat.stat.layers[i-1].nextLayerStat = testStat.stat.layers[i]
		}
	}
	check := func(src *matrix.DiscreteAxis, dst *matrix.DiscreteAxis) {
		srcValue := make([]*statUnit, 0)
		dstValue := make([]*statUnit, 0)
		for _, value := range src.Lines {
			srcValue = append(srcValue, value.Value.(*statUnit))
		}
		for _, value := range dst.Lines {
			dstValue = append(dstValue, value.Value.(*statUnit))
		}
		if src.StartKey != dst.StartKey {
			t.Fatalf("StartKey expect %s but get %s\n", src.StartKey, dst.StartKey)
		}
		for i, j := 0, 0; i < len(srcValue) && j < len(dstValue); i++ {
			if srcValue[i].Max.ReadKeys != dstValue[j].Max.ReadKeys {
				t.Fatalf("ReadKeys expect %d but get %d\n", srcValue[i].Max.ReadKeys, dstValue[j].Max.ReadKeys)
			}
			if srcValue[i].Max.ReadBytes != dstValue[j].Max.ReadBytes {
				t.Fatalf("ReadBytes expect %d but get %d\n", srcValue[i].Max.ReadBytes, dstValue[j].Max.ReadBytes)
			}
			if srcValue[i].Max.WrittenKeys != dstValue[j].Max.WrittenKeys {
				t.Fatalf("WrittenKeys expect %d but get %d\n", srcValue[i].Max.WrittenKeys, dstValue[j].Max.WrittenKeys)
			}
			if srcValue[i].Max.WrittenBytes != dstValue[j].Max.WrittenBytes {
				t.Fatalf("WrittenBytes expect %d but get %d\n", srcValue[i].Max.WrittenBytes, dstValue[j].Max.WrittenBytes)
			}
			j++
		}
	}
	// 先插三条key轴,不会超过第一个layer
	regions := [][]*regionInfo{
		{
			newRegionInfo("", encodeTablePrefix(2), 10, 20, 10, 20),
			newRegionInfo(encodeTablePrefix(2), encodeTablePrefix(3), 15, 20, 15, 30),
			newRegionInfo(encodeTablePrefix(3), encodeTablePrefix(5), 30, 30, 40, 40),
			newRegionInfo(encodeTablePrefix(5), encodeTablePrefix(7), 50, 50, 50, 50),
		},
		{
			newRegionInfo("", encodeTablePrefix(2), 15, 40, 30, 50),
			newRegionInfo(encodeTablePrefix(2), encodeTablePrefix(3), 25, 50, 65, 100),
			newRegionInfo(encodeTablePrefix(3), encodeTablePrefix(5), 130, 130, 140, 140),
			newRegionInfo(encodeTablePrefix(5), encodeTablePrefix(7), 500, 200, 550, 550),
		},
		{
			newRegionInfo("", encodeTablePrefix(2), 30, 40, 10, 20),
			newRegionInfo(encodeTablePrefix(2), encodeTablePrefix(3), 105, 200, 105, 300),
			newRegionInfo(encodeTablePrefix(3), encodeTablePrefix(5), 130, 130, 140, 140),
			newRegionInfo(encodeTablePrefix(5), encodeTablePrefix(7), 150, 150, 150, 150),
		},
	}
	for i := 0; i < len(regions); i++ {
		testStat.Append(regions[i])
	}
	for i, config := range testStat.stat.layers[0].ring {
		if i == len(regions) {
			break
		}
		check(config, newDiscreteAxis(regions[i]))

	}
	//再插十五条,此时会理论插入到第二个layer上
	for j := 0; j < 5; j++ {
		for i := 0; i < len(regions); i++ {
			testStat.Append(regions[i])
		}
	}
	compactValues := make([]*statUnit, 0)
	compactValues = append(compactValues, &statUnit{
		Max:     regionValue{30, 30, 40, 50},
		Average: regionValue{55, 90, 160, 160},
	})
	compactValues = append(compactValues, &statUnit{
		Max:     regionValue{105, 105, 200, 300},
		Average: regionValue{185, 265, 340, 560},
	})
	compactValues = append(compactValues, &statUnit{
		Max:     regionValue{130, 140, 130, 140},
		Average: regionValue{450, 500, 450, 500},
	})
	compactValues = append(compactValues, &statUnit{
		Max:     regionValue{500, 550, 200, 550},
		Average: regionValue{1250, 1350, 650, 1350},
	})
	compactLines := make([]*matrix.Line, 0)
	compactLines = append(compactLines, &matrix.Line{
		EndKey: encodeTablePrefix(2),
		Value:  compactValues[0],
	})
	compactLines = append(compactLines, &matrix.Line{
		EndKey: encodeTablePrefix(3),
		Value:  compactValues[1],
	})
	compactLines = append(compactLines, &matrix.Line{
		EndKey: encodeTablePrefix(5),
		Value:  compactValues[2],
	})
	compactLines = append(compactLines, &matrix.Line{
		EndKey: encodeTablePrefix(7),
		Value:  compactValues[3],
	})
	compactDiscreteAxies := &matrix.DiscreteAxis{
		StartKey: "",
		Lines:    compactLines,
		EndTime:  time.Now(),
	}
	check(testStat.stat.layers[1].ring[0], compactDiscreteAxies)
	head := testStat.stat.layers[0].head
	tail := testStat.stat.layers[0].tail
	length := testStat.stat.layers[0].len
	for i := head; i != tail; i = (i + 1) % length {
		check(testStat.stat.layers[0].ring[i], newDiscreteAxis(regions[i%len(regions)]))

	}
}
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
func TestLayerStat_Search(t *testing.T) {
	endTime := time.Now()
	l := layerStat{
		startTime:     time.Now(),
		ring:          make([]*matrix.DiscreteAxis, 0),
		head:          0,
		tail:          0,
		empty:         false,
		len:           15,
		compactRatio:  0,
		nextLayerStat: nil,
	}

	l.Append(&matrix.DiscreteAxis{
		StartKey: "",
		Lines:    nil,
		EndTime:  endTime.Add(-10 * time.Minute),
	})
	l.Append(&matrix.DiscreteAxis{
		StartKey: "",
		Lines:    nil,
		EndTime:  endTime.Add(-5 * time.Minute),
	})
	l.Append(&matrix.DiscreteAxis{
		StartKey: "",
		Lines:    nil,
		EndTime:  endTime.Add(-2 * time.Minute),
	})
	l.Append(&matrix.DiscreteAxis{
		StartKey: "",
		Lines:    nil,
		EndTime:  endTime.Add(-time.Minute),
	})
	l.Append(&matrix.DiscreteAxis{
		StartKey: "",
		Lines:    nil,
		EndTime:  endTime,
	})

	check := func(t *testing.T, a int, b int) {
		if a != b {
			t.Fatalf("expect index %d but get %d", a, b)
		}
	}
	find, _ := l.Search(endTime.Add(-7 * time.Minute))
	check(t, 1, find)
	find, _ = l.Search(endTime.Add(-2 * time.Minute))
	check(t, 2, find)
	find, _ = l.Search(endTime.Add(-2 * time.Minute))
	check(t, 2, find)
}
