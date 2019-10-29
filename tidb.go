package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/HunDunDM/key-visual/matrix"
	"github.com/syndtr/goleveldb/leveldb"
	"sort"
	"sync"
)

const tablePath = "store/table"

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
	tableDb *leveldb.DB
}

func loadTables() []*Table {
	tables.RLock()
	defer tables.RUnlock()
	tableSlice := make([]*Table, 0)
	if tables.tableDb == nil {
		return tableSlice
	}
	iter := tables.tableDb.NewIterator(nil, nil)
	for iter.Next() {
		value := iter.Value()
		var table Table
		err := json.Unmarshal(value, &table)
		perr(err)
		tableSlice = append(tableSlice, &table)
	}
	iter.Release()
	//err = iter.Error()

	sort.Sort(TableSlice(tableSlice))
	return tableSlice
}

func updateTables() {
	tables.Lock()
	defer tables.Unlock()
	if tables.tableDb == nil {
		return
	}
	dbInfos := dbRequest(0)
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
			err = tables.tableDb.Put(key, value, nil)
			perr(err)
		}
	}
}

func RangeTableID(newMatrix *matrix.Matrix) *matrix.Matrix {
	keys := newMatrix.Keys
	if keys == nil || len(keys) < 2 {
		return newMatrix
	}
	newMatrix.Labels = make([]*matrix.Label, 0)
	for i := 0; i < len(keys)-1; i++ {
		newMatrix.Labels = append(newMatrix.Labels, &matrix.Label{
			StartKey: keys[i],
			EndKey:   keys[i+1],
			Names:    make([]*string, 0),
		})
	}
	tbls := loadTables()
	for _, tbl := range tbls {
		dataStart := GenTableRecordPrefix(tbl.ID)
		dataEnd := GenTableRecordPrefix(tbl.ID + 1)

		start := sort.Search(len(keys), func(i int) bool {
			return keys[i] > dataStart
		})

		end := sort.Search(len(keys), func(i int) bool {
			return keys[i] >= dataEnd
		})
		if start > len(keys)-1 {
			continue
		}
		if start > 0 {
			start--
		}

		if end >= len(keys) {
			end = len(keys) - 1
		}
		for i := start; i < end; i++ {
			if dataStart < newMatrix.Labels[i].StartKey && dataEnd > newMatrix.Labels[i].EndKey {
				newMatrix.Labels[i].StartKey = dataStart
				newMatrix.Labels[i].EndKey = dataEnd
			}
			name := fmt.Sprintf("tidb:%s, table:%s, data", tbl.DB, tbl.Name)
			newMatrix.Labels[i].Names = append(newMatrix.Labels[i].Names, &name)
		}
		for idx, idxName := range tbl.Indices {

			indexStart := GenTableIndexPrefix(tbl.ID, idx)
			indexEnd := GenTableIndexPrefix(tbl.ID, idx+1)
			//fmt.Println(indexStart, "aaaaaaaaaa", tbl.ID, idx)
			//fmt.Println(indexEnd, "aaaaaaaaa", tbl.ID, idx)
			start := sort.Search(len(keys), func(i int) bool {
				return keys[i] > indexStart
			})

			end := sort.Search(len(keys), func(i int) bool {
				return keys[i] >= indexEnd
			})

			if start > len(keys)-1 {
				continue
			}
			if start > 0 {
				start--
			}
			if end >= len(keys) {
				end = len(keys) - 1
			}

			for i := start; i < end; i++ {
				if indexStart < newMatrix.Labels[i].StartKey && indexEnd > newMatrix.Labels[i].EndKey {
					newMatrix.Labels[i].StartKey = indexStart
					newMatrix.Labels[i].EndKey = indexEnd
				}
				name := fmt.Sprintf("tidb:%s, table:%s, index:%s", tbl.DB, tbl.Name, idxName)
				newMatrix.Labels[i].Names = append(newMatrix.Labels[i].Names, &name)
			}
		}
	}
	return newMatrix
}

var tables TablesStore

func init() {
	db, err := leveldb.OpenFile(tablePath, nil)
	perr(err)
	tables.tableDb = db
}

func (s *TablesStore) Close() {
	s.Lock()
	s.tableDb.Close()
	s.tableDb = nil
	s.Unlock()
}
