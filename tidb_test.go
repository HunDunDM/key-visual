package main

import (
	"github.com/pingcap/goleveldb/leveldb"
	"reflect"
	"testing"
)
const testtablepath = "test/table"
func TestUpdateAndLoadTables(t *testing.T) {
	tables.LeveldbStorage, _ = NewLeveldbStorage(testtablepath)
	updateTables()
	tablesBefore := loadTables()
	tables.LeveldbStorage.Close()
	db, err := leveldb.OpenFile(testtablepath, nil)
	perr(err)
	tables.LeveldbStorage = &LeveldbStorage{db}

	tablesAfter := loadTables()

	if !reflect.DeepEqual(tablesBefore, tablesAfter) {
		t.Fatalf("expect\n%v\nbut got\n%v", tablesBefore, tablesAfter)
	}
}