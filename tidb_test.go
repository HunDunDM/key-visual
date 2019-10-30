package main

import (
	"github.com/pingcap/goleveldb/leveldb"
	"reflect"
	"testing"
	"time"
)

const testtablepath = "../test/table"

func TestUpdateAndLoadTables(t *testing.T) {
	time.Sleep(time.Second)
	tables.LeveldbStorage, _ = NewLeveldbStorage(testtablepath)
	updateTables()
	tablesBefore := loadTables()
	tables.Close()
	db, err := leveldb.OpenFile(testtablepath, nil)
	perr(err)
	tables.LeveldbStorage = &LeveldbStorage{db}

	tablesAfter := loadTables()

	if !reflect.DeepEqual(tablesBefore, tablesAfter) {
		t.Fatalf("expect\n%v\nbut got\n%v", tablesBefore, tablesAfter)
	}
	tables.LeveldbStorage.Close()
}

func TestTableSlice_Len(t *testing.T) {
	var tableSlice TableSlice
	tableSlice = append(tableSlice, &Table{
		Name:    "aa",
		DB:      "a",
		ID:      0,
		Indices: nil,
	})
	tableSlice = append(tableSlice, &Table{
		Name:    "ab",
		DB:      "b",
		ID:      1,
		Indices: nil,
	})
	if tableSlice.Len() != 2 {
		t.Fatalf("error len, expect 2 but get %d", tableSlice.Len())
	}
}
func TestTableSlice_Swap(t *testing.T) {
	var tableSlice TableSlice
	tableSlice = append(tableSlice, &Table{
		Name:    "aa",
		DB:      "a",
		ID:      0,
		Indices: nil,
	})
	tableSlice = append(tableSlice, &Table{
		Name:    "ab",
		DB:      "b",
		ID:      1,
		Indices: nil,
	})
	tableSlice.Swap(0, 1)
	if tableSlice[0].ID != 1 {
		t.Fatalf("error swap")
	}
}
func TestTableSlice_Less(t *testing.T) {
	var tableSlice TableSlice
	tableSlice = append(tableSlice, &Table{
		Name:    "aa",
		DB:      "a",
		ID:      0,
		Indices: nil,
	})
	tableSlice = append(tableSlice, &Table{
		Name:    "ab",
		DB:      "b",
		ID:      1,
		Indices: nil,
	})
	if tableSlice.Less(0, 1) != true {
		t.Fatalf("error less")
	}
}
