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
		perr(err)
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
func (db *LeveldbStorage) Search(k []byte) iterator.Iterator {
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		if string(iter.Key()) >= string(k) {
			return iter
		}
	}
	iter.Release()
	return nil
}

// Traversal return a traversal of the storage
func (db *LeveldbStorage) Traversal() (allValues []string) {
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		allValues = append(allValues, string(iter.Value()))
	}
	iter.Release()
	return allValues
}

// Range gets a range of value for a given key range.
func (db *LeveldbStorage) LoadRange(startKey, endKey []byte) ([]string, []string, error) {
	startIter := db.Search(startKey)
	endIter := db.Search(endKey)

	isEndNil := false
	if startIter == nil {
		return nil, nil, errors.New("startTime too late")
	}
	if endIter == nil {
		isEndNil = true
	}
	iter := startIter
	keys := make([]string, 0)
	values := make([]string, 0)
	keys = append(keys, string(iter.Key()))
	values = append(values, string(iter.Value()))
	for iter.Next() {
		keys = append(keys, string(iter.Key()))
		values = append(values, string(iter.Value()))
		if !isEndNil && string(iter.Key()) == string(endIter.Key()) {
			break
		}
	}
	iter.Release()
	return keys, values, nil
}
