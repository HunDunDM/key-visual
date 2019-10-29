package main

import (
	"fmt"
	"reflect"
	"testing"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestUpdateAndLoadTables(t *testing.T) {
	updateTables()
	tablesBefore := loadTables()
	tables.Close()
	db, err := leveldb.OpenFile(tablePath, nil)
	perr(err)
	tables.tableDb = db

	tablesAfter := loadTables()
	for _, table := range tablesAfter {
		fmt.Println(table)
	}

	if !reflect.DeepEqual(tablesBefore, tablesAfter) {
		t.Fatalf("expect\n%v\nbut got\n%v", tablesBefore, tablesAfter)
	}
}