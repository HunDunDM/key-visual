package main

import (
	"errors"
	"github.com/pingcap/goleveldb/leveldb"
	"github.com/pingcap/goleveldb/leveldb/iterator"
)

type LeveldbRegion struct {
	*leveldb.DB
}

// NewLeveldbKV is used to store regions information.
func NewLeveldbRegion(path string) (*LeveldbRegion, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &LeveldbRegion{db}, nil
}

// Load gets a value for a given key.
func (kv *LeveldbRegion) Load(key []byte) (string, error) {
	v, err := kv.Get(key, nil)
	if err != nil {
		return "", err
	}
	return string(v), err
}

// Save stores a key-value pair.
func (kv *LeveldbRegion) Save(key, value []byte) error {
	return kv.Put(key, value, nil)
}
func (kv *LeveldbRegion) searchRegion(k []byte) iterator.Iterator {
	iter := kv.NewIterator(nil, nil)
	for iter.Next() {
		if string(iter.Key()) < string(k) {
			return iter
		}
	}
	iter.Release()
	return nil
}

// Range gets a range of value for a given key range.
func (kv *LeveldbRegion) LoadRange(startKey, endKey []byte, limit int) ([]string, []string, error) {
	startIter := kv.searchRegion(startKey)
	endIter := kv.searchRegion(endKey)
	if endIter == nil {
		return nil, nil, errors.New("endTime too early")
	}
	isStartNil := false
	if startIter == nil {
		isStartNil = true
	}
	iter := endIter
	keys := make([]string, 0, limit)
	values := make([]string, 0, limit)
	count := 0
	for iter.Next() {
		if count >= limit {
			break
		}
		keys = append(keys, string(iter.Key()))
		values = append(values, string(iter.Value()))
		count++
		if !isStartNil && string(iter.Key()) == string(startIter.Key()) {
			break
		}
	}
	iter.Release()
	return keys, values, nil
}
