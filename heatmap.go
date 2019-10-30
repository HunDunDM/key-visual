package main

import (
	"fmt"
	"github.com/HunDunDM/key-visual/matrix"
	"sort"
	"time"
)

type Label struct {
	StartKey string    `json:"start_key"`
	EndKey   string    `json:"end_key"`
	Names    []*string `json:"labels"`
}

type Heatmap struct {
	Data   [][]interface{} `json:"data"`   // two-dimensional data matrix
	Keys   []string        `json:"keys"`   // Y-axis of heatmap
	Times  []time.Time     `json:"times"`  // X-axis of heatmap
	Labels []*Label        `json:"labels"` // the label information at the left of heatmap indicating tables
}

type MultiValue struct {
	WrittenBytes uint64 `json:"written_bytes"`
	ReadBytes    uint64 `json:"read_bytes"`
	WrittenKeys  uint64 `json:"written_keys"`
	ReadKeys     uint64 `json:"read_keys"`
}

// a storage unit of regionInfo, which needs to implement matrix.Value interface
type MultiUnit struct {
	// calculate average and maximum simultaneously
	Max     MultiValue `json:"max"`
	Average MultiValue `json:"average"`
}

// 返回两个数中的较大值
func Max(a uint64, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func (v *MultiUnit) Split(count int) matrix.Value {
	countU64 := uint64(count)
	res := *v
	res.Average.ReadKeys /= countU64
	res.Average.ReadBytes /= countU64
	res.Average.WrittenKeys /= countU64
	res.Average.WrittenBytes /= countU64
	return &res
}

func (v *MultiUnit) Merge(other matrix.Value) {
	v2 := other.(*MultiUnit)
	v.Max.WrittenBytes = Max(v.Max.WrittenBytes, v2.Max.WrittenBytes)
	v.Max.WrittenKeys = Max(v.Max.WrittenKeys, v2.Max.WrittenKeys)
	v.Max.ReadBytes = Max(v.Max.ReadBytes, v2.Max.ReadBytes)
	v.Max.ReadKeys = Max(v.Max.ReadKeys, v2.Max.ReadKeys)
	v.Average.WrittenBytes = v.Average.WrittenBytes + v2.Average.WrittenBytes
	v.Average.WrittenKeys = v.Average.WrittenKeys + v2.Average.WrittenKeys
	v.Average.ReadBytes = v.Average.ReadBytes + v2.Average.ReadBytes
	v.Average.ReadKeys = v.Average.ReadKeys + v2.Average.ReadKeys
}

func (v *MultiUnit) Useless(threshold uint64) bool {
	return Max(v.Max.ReadBytes, v.Max.WrittenBytes) < threshold
}

func (v *MultiUnit) GetThreshold() uint64 {
	return Max(v.Max.ReadBytes, v.Max.WrittenBytes)
}

func (v *MultiUnit) Clone() matrix.Value {
	statUnitClone := *v
	return &statUnitClone
}

func (v *MultiUnit) Reset() {
	*v = MultiUnit{}
}

func (v *MultiUnit) Default() matrix.Value {
	return new(MultiUnit)
}

func (v *MultiUnit) Equal(other matrix.Value) bool {
	another := other.(*MultiUnit)
	return *v == *another
}

// a statistics unit of single index, which needs to implement matrix.Value interface
type SingleUnit struct {
	// calculate average and maximum simultaneously
	// 0 indicates maximum mode, 1 indicates average mode
	Value uint64 `json:"value"`
	Mode  int    `json:"mode"`
}

func (v *SingleUnit) Split(count int) matrix.Value {
	countU64 := uint64(count)
	res := *v
	if v.Mode == 1 {
		res.Value /= countU64
	}
	return &res
}

func (v *SingleUnit) Merge(other matrix.Value) {
	v2 := other.(*SingleUnit)
	if v.Mode == 0 {
		v.Value = Max(v.Value, v2.Value)
	} else {
		v.Value = v.Value + v2.Value
	}
}

func (v *SingleUnit) Useless(threshold uint64) bool {
	return v.Value < threshold
}

func (v *SingleUnit) GetThreshold() uint64 {
	return v.Value
}

func (v *SingleUnit) Clone() matrix.Value {
	statUnitClone := *v
	return &statUnitClone
}

func (v *SingleUnit) Reset() {
	*v = SingleUnit{
		Mode: v.Mode,
	}
}

func (v *SingleUnit) Default() matrix.Value {
	return &SingleUnit{
		Mode: v.Mode,
	}
}

func (v *SingleUnit) Equal(other matrix.Value) bool {
	another := other.(*SingleUnit)
	return *v == *another
}

func generateHeatmap(startTime time.Time, endTime time.Time, startKey string, endKey string, tag, mode string) *Heatmap {
	separateValue := func(unit *regionUnit) matrix.Value {
		var m int
		switch mode {
		case "average":
			m = 1
		default:
			m = 0
		}

		var data uint64
		switch tag {
		case "read_bytes":
			data = unit.Max.ReadBytes
		case "written_bytes":
			data = unit.Max.WrittenBytes
		case "read_keys":
			data = unit.Max.ReadKeys
		case "written_keys":
			data = unit.Max.WrittenKeys
		case "read_and_written_bytes":
			data = unit.Max.ReadBytes + unit.Max.WrittenBytes
		case "read_and_written_keys":
			data = unit.Max.ReadKeys + unit.Max.WrittenKeys
		default:
			return unit.BuildMultiValue()
		}
		single := &SingleUnit{
			Value: data,
			Mode:  m,
		}
		return single
	}
	rangePlane := globalRegionStore.Range(startTime, endTime, separateValue)
	if rangePlane == nil {
		return nil
	}
	// range information in key axis
	for i := 0; i < len(rangePlane.Axes); i++ {
		tempAxis := rangePlane.Axes[i]
		if tempAxis != nil {
			rangePlane.Axes[i] = tempAxis.Range(startKey, endKey)
		}
	}

	newMatrix := rangePlane.Pixel(50, 80)
	heatmap := ChangeIntoHeatmap(newMatrix)
	return MatchTable(heatmap)
}

func ChangeIntoHeatmap(matrix *matrix.Matrix) *Heatmap {
	if matrix == nil || len(matrix.Data) == 0 || len(matrix.Data[0]) == 0 {
		return nil
	}
	heatmap := &Heatmap{
		Keys:  matrix.Keys,
		Times: matrix.Times,
	}
	isMulti := true
	if _, ok := matrix.Data[0][0].(*SingleUnit); ok {
		isMulti = false
	}
	if isMulti {
		n := len(matrix.Data)
		heatmap.Data = make([][]interface{}, n)
		for i := 0; i < n; i++ {
			m := len(matrix.Data[i])
			heatmap.Data[i] = make([]interface{}, m)
			for j := 0; j < m; j++ {
				heatmap.Data[i][j] = matrix.Data[i][j]
			}
		}
	} else {
		n := len(matrix.Data)
		heatmap.Data = make([][]interface{}, n)
		for i := 0; i < n; i++ {
			m := len(matrix.Data[i])
			heatmap.Data[i] = make([]interface{}, m)
			for j := 0; j < m; j++ {
				singleUnit := matrix.Data[i][j].(*SingleUnit)
				heatmap.Data[i][j] = singleUnit.Value
			}
		}
	}
	return heatmap
}

// match tables
func MatchTable(hmap *Heatmap) *Heatmap {
	if hmap == nil {
		return nil
	}
	keys := hmap.Keys
	if keys == nil || len(keys) < 2 {
		return hmap
	}
	hmap.Labels = make([]*Label, 0)
	for i := 0; i < len(keys)-1; i++ {
		hmap.Labels = append(hmap.Labels, &Label{
			StartKey: keys[i],
			EndKey:   keys[i+1],
			Names:    make([]*string, 0),
		})
	}
	tables := loadTables()
	for _, table := range tables {
		dataStart := GenTableRecordPrefix(table.ID)
		dataEnd := GenTableRecordPrefix(table.ID + 1)

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
			if dataStart < hmap.Labels[i].StartKey && dataEnd > hmap.Labels[i].EndKey {
				hmap.Labels[i].StartKey = dataStart
				hmap.Labels[i].EndKey = dataEnd
			}
			name := fmt.Sprintf("tidb:%s, table:%s, data", table.DB, table.Name)
			hmap.Labels[i].Names = append(hmap.Labels[i].Names, &name)
		}
		for idx, idxName := range table.Indices {

			indexStart := GenTableIndexPrefix(table.ID, idx)
			indexEnd := GenTableIndexPrefix(table.ID, idx+1)
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
				if indexStart < hmap.Labels[i].StartKey && indexEnd > hmap.Labels[i].EndKey {
					hmap.Labels[i].StartKey = indexStart
					hmap.Labels[i].EndKey = indexEnd
				}
				name := fmt.Sprintf("tidb:%s, table:%s, index:%s", table.DB, table.Name, idxName)
				hmap.Labels[i].Names = append(hmap.Labels[i].Names, &name)
			}
		}
	}
	return hmap
}
