package main

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/HunDunDM/key-visual/matrix"
	"github.com/pingcap/goleveldb/leveldb"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
)

const teststatpath = "../test/stat"
const testrangepath = "../test/range"

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

func newDiscreteAxis(regions []*regionInfo) *DiscreteAxis {
	axis := &DiscreteAxis{
		StartKey: regions[0].StartKey,
		EndTime:  time.Now(),
	}
	//生成lines
	for _, info := range regions {
		line := &Line{
			EndKey:     info.EndKey,
			RegionUnit: newRegionUnit(info),
		}
		axis.Lines = append(axis.Lines, line)
	}
	//对lins的value小于1（即为0）的线段压缩
	axis.DeNoise(1)
	return axis
}

func TestRegionStore_Append(t *testing.T) {
	globalRegionStore.LeveldbStorage, _ = NewLeveldbStorage(teststatpath)
	testRegions := make([][]*regionInfo, 0)
	regions := []*regionInfo{
		newRegionInfo(encodeTablePrefix(1), encodeTablePrefix(2), 10, 20, 20, 30),
		newRegionInfo(encodeTablePrefix(2), encodeTablePrefix(3), 10, 20, 20, 30),
		newRegionInfo(encodeTablePrefix(3), encodeTablePrefix(5), 10, 20, 20, 30),
	}
	testRegions = append(testRegions, regions)
	regions = []*regionInfo{
		newRegionInfo(encodeTablePrefix(1), encodeTablePrefix(2), 20, 30, 20, 30),
		newRegionInfo(encodeTablePrefix(2), encodeTablePrefix(3), 70, 20, 20, 30),
		newRegionInfo(encodeTablePrefix(3), encodeTablePrefix(5), 10, 20, 20, 30),
	}
	testRegions = append(testRegions, regions)
	regions = []*regionInfo{
		newRegionInfo(encodeTablePrefix(1), encodeTablePrefix(2), 25, 0, 20, 0),
		newRegionInfo(encodeTablePrefix(2), encodeTablePrefix(3), 55, 20, 20, 130),
		newRegionInfo(encodeTablePrefix(3), encodeTablePrefix(5), 10, 200, 20, 300),
	}
	testRegions = append(testRegions, regions)
	for _, region := range testRegions {
		globalRegionStore.Append(region)
	}
	valuesBefore := globalRegionStore.Traversal()
	globalRegionStore.Close()
	db, err := leveldb.OpenFile(teststatpath, nil)
	perr(err)
	globalRegionStore.LeveldbStorage = &LeveldbStorage{db}
	defer globalRegionStore.LeveldbStorage.Close()
	valuesAfter := globalRegionStore.Traversal()
	if !reflect.DeepEqual(valuesBefore, valuesAfter) {
		t.Fatalf("expect\n%v\nbut got\n%v", valuesBefore, valuesAfter)
	}

}
func TestRegionStore_Range(t *testing.T) {
	globalRegionStore.LeveldbStorage, _ = NewLeveldbStorage(testrangepath)
	testRegions := make([][]*regionInfo, 0)
	regions := []*regionInfo{
		newRegionInfo(encodeTablePrefix(1), encodeTablePrefix(2), 10, 20, 20, 30),
		newRegionInfo(encodeTablePrefix(2), encodeTablePrefix(3), 10, 20, 20, 30),
		newRegionInfo(encodeTablePrefix(3), encodeTablePrefix(5), 10, 20, 20, 30),
	}
	testRegions = append(testRegions, regions)
	regions = []*regionInfo{
		newRegionInfo(encodeTablePrefix(1), encodeTablePrefix(2), 20, 30, 20, 30),
		newRegionInfo(encodeTablePrefix(2), encodeTablePrefix(3), 70, 20, 20, 30),
		newRegionInfo(encodeTablePrefix(3), encodeTablePrefix(5), 10, 20, 20, 30),
	}
	testRegions = append(testRegions, regions)
	regions = []*regionInfo{
		newRegionInfo(encodeTablePrefix(1), encodeTablePrefix(2), 25, 0, 20, 0),
		newRegionInfo(encodeTablePrefix(2), encodeTablePrefix(3), 55, 20, 20, 130),
		newRegionInfo(encodeTablePrefix(3), encodeTablePrefix(5), 10, 200, 20, 300),
	}
	testRegions = append(testRegions, regions)
	for _, region := range testRegions {
		globalRegionStore.Append(region)
		time.Sleep(time.Second)
	}
	f := func(unit *regionUnit) matrix.Value {
		return unit.BuildMultiValue()
	}
	plane := globalRegionStore.Range(time.Now().Add(-time.Minute), time.Now(), f)
	if plane.Axes[0].StartKey != encodeTablePrefix(1) {
		t.Fatalf("error range, expect %s but get %s", encodeTablePrefix(1), plane.Axes[0].StartKey)
	}
}

func TestScanRegions(t *testing.T) {
	regions := ScanRegions()
	newRegions := ScanRegions()
	if regions == nil || len(regions) == 0 || newRegions == nil || len(newRegions) == 0 {
		t.Fatalf("error scan regions")
	}
	if !reflect.DeepEqual(regions, newRegions) {
		fmt.Printf("two scan not the same, before: \n%v\n, after: \n%v\n", regions, newRegions)
	}
}
func TestRegionUnit_Merge(t *testing.T) {
	r := &regionUnit{
		Max: regionData{
			10, 20, 30, 40,
		},
		Average: regionData{
			20, 30, 40, 50,
		},
	}
	d := &regionUnit{
		Max: regionData{
			55, 25, 15, 35,
		},
		Average: regionData{
			10, 20, 45, 55,
		},
	}
	r.Merge(d)
	d = &regionUnit{
		Max: regionData{
			55, 25, 30, 40,
		},
		Average: regionData{
			30, 50, 85, 105,
		},
	}
	if !reflect.DeepEqual(r, d) {
		t.Fatalf("expect \n%v\n but get \n%v\n", d, r)
	}
}
func TestRegionUnit_Useless(t *testing.T) {
	r := &regionUnit{
		Max: regionData{
			10, 20, 30, 40,
		},
		Average: regionData{
			20, 30, 40, 50,
		},
	}
	if r.Useless(100) != true {
		t.Fatalf("expect true but get false")
	} else if r.Useless(10) != false {
		t.Fatalf("expect false but get true")
	}
}
func TestRegionUnit_BuildMultiValue(t *testing.T) {
	r := regionUnit{
		Max: regionData{
			10, 20, 30, 40,
		},
		Average: regionData{
			10, 20, 30, 40,
		},
	}
	d := r.BuildMultiValue()
	if d.Max.ReadBytes != r.Max.ReadBytes || d.Max.ReadKeys != r.Max.ReadKeys || d.Max.WrittenBytes != r.Max.WrittenBytes ||
		d.Max.WrittenKeys != r.Max.WrittenKeys || d.Average.ReadBytes != r.Average.ReadBytes || d.Average.ReadKeys != r.Average.ReadKeys ||
		d.Average.WrittenKeys != r.Average.WrittenKeys || d.Average.WrittenBytes != d.Average.WrittenBytes {
		t.Fatalf("error build multiValue")
	}
}
func TestDiscreteAxis_DeNoise(t *testing.T) {
	regions := []*regionInfo{
		{1, "", "a", 1, 2, 3, 4},
		{2, "a", "b", 1, 2, 3, 4},
		{3, "b", "c", 15, 20, 25, 30},
	}
	axis := &DiscreteAxis{
		StartKey: regions[0].StartKey,
		EndTime:  time.Now(),
	}
	// generate lines
	for _, info := range regions {
		if info == nil {
			continue
		}
		line := &Line{
			EndKey:     info.EndKey,
			RegionUnit: newRegionUnit(info),
		}
		axis.Lines = append(axis.Lines, line)
	}
	axis.DeNoise(10)
	if axis.Lines[0].EndKey != "b" || axis.Lines[1].EndKey != "c" {
		t.Fatalf("error denoise")
	}
}
