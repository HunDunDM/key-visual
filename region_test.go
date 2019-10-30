package main

import (
	"encoding/hex"
	"reflect"
	"testing"
	"time"

	"github.com/pingcap/goleveldb/leveldb"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"

)
const teststatpath = "../test/stat"
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
			EndKey: info.EndKey,
			regionUnit: newRegionUnit(info),
		}
		axis.Lines = append(axis.Lines, line)
	}
	//对lins的value小于1（即为0）的线段压缩
	axis.DeNoise(1)
	return axis
}

func TestStat_Append(t *testing.T) {
	globalRegionStore.LeveldbStorage, _ = NewLeveldbStorage(testtablepath)
	testRegions := make([][]*regionInfo, 0)
	regions := []*regionInfo {
		newRegionInfo(encodeTablePrefix(1), encodeTablePrefix(2), 10, 20, 20, 30),
		newRegionInfo(encodeTablePrefix(2), encodeTablePrefix(3), 10, 20, 20, 30),
		newRegionInfo(encodeTablePrefix(3), encodeTablePrefix(5), 10, 20, 20, 30),
	}
	testRegions = append(testRegions, regions)
	regions = []*regionInfo {
		newRegionInfo(encodeTablePrefix(1), encodeTablePrefix(2), 20, 30, 20, 30),
		newRegionInfo(encodeTablePrefix(2), encodeTablePrefix(3), 70, 20, 20, 30),
		newRegionInfo(encodeTablePrefix(3), encodeTablePrefix(5), 10, 20, 20, 30),
	}
	testRegions = append(testRegions, regions)
	regions = []*regionInfo {
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
	db, err := leveldb.OpenFile(testtablepath, nil)
	perr(err)
	globalRegionStore.LeveldbStorage = &LeveldbStorage{db}
	defer globalRegionStore.LeveldbStorage.Close()
	valuesAfter := globalRegionStore.Traversal()
	if !reflect.DeepEqual(valuesBefore, valuesAfter) {
		t.Fatalf("expect\n%v\nbut got\n%v", valuesBefore, valuesAfter)
	}

}
func TestStat_RangeMatrix(t *testing.T) {

}


func TestScanRegions(t *testing.T) {
	regions := scanRegions()
	newRegions := scanRegions()
	if regions == nil || len(regions) == 0 || newRegions == nil || len(newRegions) == 0 {
		t.Fatalf("error scan regions")
	}
	if !reflect.DeepEqual(regions, newRegions) {
		t.Fatalf("two scan not the same, before: \n%v\n, after: \n%v\n", regions, newRegions)
	}
}




