package main

import (
	"errors"
	"github.com/pingcap/goleveldb/leveldb"
	"github.com/pingcap/goleveldb/leveldb/iterator"
)

type LeveldbStorage struct {
	*leveldb.DB
}

// NewLeveldbStorage is used to store regions information.
func NewLeveldbStorage(path string) (*LeveldbStorage, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &LeveldbStorage{db}, nil
}

// Load gets a value for a given key.
func (db *LeveldbStorage) Load(key []byte) (string, error) {
	v, err := db.Get(key, nil)
	if err != nil {
		return "", err
	}
	return string(v), err
}

// Save stores a key-value pair.
func (db *LeveldbStorage) Save(key, value []byte) error {
	return db.Put(key, value, nil)
}
func (db *LeveldbStorage) search(k []byte) iterator.Iterator {
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		if string(iter.Key()) < string(k) {
			return iter
		}
	}
	iter.Release()
	return nil
}
func (db *LeveldbStorage) traversal() (allValues []string) {
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		allValues = append(allValues, string(iter.Value()))
	}
	iter.Release()
	return allValues
}

// Range gets a range of value for a given key range.
func (db *LeveldbStorage) LoadRange(startKey, endKey []byte, limit int) ([]string, []string, error) {
	startIter := db.search(startKey)
	endIter := db.search(endKey)
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
