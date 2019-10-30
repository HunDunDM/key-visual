package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

const defaulttablePath = "storage/table"

// Table saves the info of a table
type Table struct {
	Name string `json:"name"`
	DB   string `json:"db"`
	ID   int64  `json:"id"`

	Indices map[int64]string `json:"indices"`
}

func (t *Table) String() string {
	return fmt.Sprintf("%s.%s", t.DB, t.Name)
}

// TableSlice is the slice of tables
type TableSlice []*Table

func (s TableSlice) Len() int      { return len(s) }
func (s TableSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s TableSlice) Less(i, j int) bool {
	if s[i].DB < s[j].DB {
		return true
	} else if s[i].DB == s[j].DB && s[i].Name < s[j].Name {
		return true
	} else if s[i].DB == s[j].DB && s[i].Name == s[j].Name {
		return s[i].ID < s[j].ID
	}
	return false
}

type TablesStore struct {
	sync.RWMutex
	*LeveldbStorage
}

func loadTables() []*Table {
	tableSlice := make([]*Table, 0)
	tables.RLock()
	allValue := tables.Traversal()
	tables.RUnlock()
	for _, v := range allValue {
		var table Table
		err := json.Unmarshal([]byte(v), &table)
		perr(err)
		tableSlice = append(tableSlice, &table)
	}
	sort.Sort(TableSlice(tableSlice))
	return tableSlice
}

func updateTables() {
	dbInfos := dbRequest(0)
	tables.Lock()
	defer tables.Unlock()
	for _, info := range dbInfos {
		if info.State == 0 {
			continue
		}
		tblInfos := tableRequest(0, info.Name.O)

		for _, table := range tblInfos {
			indices := make(map[int64]string, len(table.Indices))
			for _, index := range table.Indices {
				indices[index.ID] = index.Name.O
			}
			newTable := &Table{
				ID:      table.ID,
				Name:    table.Name.O,
				DB:      info.Name.O,
				Indices: indices,
			}

			value, err := json.Marshal(newTable)
			perr(err)
			var key = make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(newTable.ID))
			err = tables.Save(key, value)
			perr(err)
		}
	}
}

var tables TablesStore

func init() {
	tables.LeveldbStorage, _ = NewLeveldbStorage(defaulttablePath)
}
