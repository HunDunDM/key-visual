package main

import (
	"fmt"
	"github.com/HunDunDM/key-visual/matrix"
	"sort"
	"sync"
)

// Table saves the info of a table
type Table struct {
	Name string
	DB   string
	ID   int64

	Indices map[int64]string
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

// id -> map
var tables = sync.Map{}

func loadTables() []*Table {
	tableSlice := make([]*Table, 0, 1024)

	tables.Range(func(_key, value interface{}) bool {
		table := value.(*Table)
		tableSlice = append(tableSlice, table)
		return true
	})

	sort.Sort(TableSlice(tableSlice))
	return tableSlice
}

func updateTables() {
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
			tables.Store(table.ID, newTable)
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
	updateTables()
	tbls := loadTables()
	for _, tbl := range tbls {
		dataStart := GenTableRecordPrefix(tbl.ID)
		dataEnd := GenTableRecordPrefix(tbl.ID + 1)

		//fmt.Println(dataStart, "*****", tbl.ID)
		//fmt.Println(dataEnd, "*****", tbl.ID)
		//for _, key := range keys {
		//	fmt.Println(key, "kkkkkk")
		//}
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
