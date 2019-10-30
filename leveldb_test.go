package main

import (
	"github.com/pingcap/goleveldb/leveldb"
	"reflect"
	"testing"
)
var keys = []string{
	"aaaq325hdjjhsf",
	"bbbczvafagsheerqatw",
	"cccafshdgahs",
	"d1122233aaa",
	"e223344444",
}
var values = []string{
	"111tw6u6i7jybeahtnyrjum",
	"222hansyjeww",
	"3334523473562374837564q623748",
	"aaaaaaaaaaaaaa",
	"bbbbbbbbbbbb",
}
func TestNewLeveldbStorage(t *testing.T) {
	db, err := NewLeveldbStorage("test/store/new")
	defer db.Close()
	perr(err)
	db.Put([]byte("aa"), []byte("111"), nil)
	v, err := db.Get([]byte("aa"), nil)
	perr(err)
	s := string(v)
	if s != "111" {
		t.Fatalf("expect 111 but get %s", s)
	}

}
func TestLeveldbStorage_Save(t *testing.T) {
	db, err := NewLeveldbStorage("test/store/save")
	perr(err)
	for i := range keys {
		err := db.Save([]byte(keys[i]), []byte(values[i]))
		perr(err)
	}
	db.Close()
	newDb, newErr := leveldb.OpenFile("test/store/save", nil)
	defer newDb.Close()
	perr(newErr)
	iter := newDb.NewIterator(nil, nil)
	i := 0
	for iter.Next() {
		if string(iter.Key()) != keys[i] {
			t.Fatalf("expect key %s but get %s", keys[i], string(iter.Key()))
		} else if string(iter.Value()) != values[i] {
			t.Fatalf("expect value %s but get %s", values[i], string(iter.Value()))
		}
		i++
	}
}
func TestLeveldbStorage_Load(t *testing.T) {
	db, err := leveldb.OpenFile("..test/store/save", nil)
	perr(err)
	for i := range keys {
		err := db.Put([]byte(keys[i]), []byte(values[i]), nil)
		perr(err)
	}
	db.Close()
	newDb, newErr := NewLeveldbStorage("test/store/save")
	defer newDb.Close()
	perr(newErr)
	v, newErrLoad := newDb.Load([]byte(keys[0]))
	perr(newErrLoad)
	if v != values[0] {
		t.Fatalf("expect %s but get %s", values[0], v)
	}
	v, err = newDb.Load([]byte("aa"))
	if err == nil {
		t.Fatalf("aa should not be found, expect error but get none")
	}
}
func TestLeveldb_Search(t *testing.T) {
	db, err := leveldb.OpenFile("test/store/search", nil)
	perr(err)
	for i := range keys {
		err := db.Put([]byte(keys[i]), []byte(values[i]), nil)
		perr(err)
	}
	db.Close()
	newDb, newErr := NewLeveldbStorage("test/store/save")
	defer newDb.Close()
	perr(newErr)
	iter := newDb.Search([]byte("aabb"))
	newIter := newDb.NewIterator(nil, nil)
	newIter.Next()
	newIter.Next()
	if string(newIter.Key()) != string(iter.Key()) || string(newIter.Value()) != string(iter.Value()) {
		t.Fatalf("error search, expect key: %s, value: %s; but get key: %s, value: %s", string(newIter.Key()), string(newIter.Value()), string(iter.Key()), string(iter.Value()))
	}
}
func TestLeveldbStorage_Traversal(t *testing.T) {
	db, err := leveldb.OpenFile("test/store/traversal", nil)
	perr(err)
	for i := range keys {
		err := db.Put([]byte(keys[i]), []byte(values[i]), nil)
		perr(err)
	}
	db.Close()
	newDb, newErr := NewLeveldbStorage("test/store/save")
	defer newDb.Close()
	perr(newErr)
	newValues := newDb.Traversal()
	if !reflect.DeepEqual(newValues, values) {
		t.Fatalf("expect\n%v\nbut got\n%v", values, newValues)
	}
}
func TestLeveldbStorage_LoadRange(t *testing.T) {
	db, err := leveldb.OpenFile("test/store/loadrange", nil)
	perr(err)
	for i := range keys {
		err := db.Put([]byte(keys[i]), []byte(values[i]), nil)
		perr(err)
	}
	db.Close()
	newDb, newErr := NewLeveldbStorage("test/store/loadrange")
	defer newDb.Close()
	perr(newErr)
	newKeys, newValues, newErr := newDb.LoadRange([]byte(keys[1]), []byte(keys[4]))
	perr(newErr)
	if !reflect.DeepEqual(newValues, values[1:5]) || !reflect.DeepEqual(newKeys, keys[1:5]) {
		t.Fatalf("error loadrange, get keys:%v", newKeys)
	}
}