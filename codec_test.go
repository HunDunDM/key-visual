package main

import (
	"testing"
)

func TestEncodeBytes(t *testing.T) {
	a := EncodeBytes([]byte("aaa"))
	b := EncodeBytes([]byte("bbb"))
	newA := []byte{97, 97, 97, 0, 0, 0, 0, 0, 250}
	newB := []byte{98, 98, 98, 0, 0, 0, 0, 0, 250}
	if string(a) != string(newA) || string(b) != string(newB) {
		t.Fatalf("error EncodeBytes")
	}
}

func TestEncodeIntToCmpUint(t *testing.T) {
	if EncodeIntToCmpUint(1) != 9223372036854775809 {
		t.Fatalf("error EncodeIntToCmpUint")
	}
}

func TestEncodeInt(t *testing.T) {
	byteA := []byte{97, 97, 97, 128, 0, 0, 0, 0, 0, 0, 1}
	byteB := []byte{97, 97, 97, 128, 0, 0, 0, 0, 0, 0, 2}
	if string(EncodeInt([]byte("aaa"), 1)) != string(byteA) || string(EncodeInt([]byte("aaa"), 2)) != string(byteB) {
		t.Fatalf("error EncodeInt")
	}
}
func TestGenTableRecordPrefix(t *testing.T) {
	if GenTableRecordPrefix(1) != "7480000000000000FF015F720000000000FA" {
		t.Fatalf("error GenTableRecordPrefix, expect 7480000000000000FF015F720000000000FA but get %s", GenTableRecordPrefix(1))
	}
}
func TestGenTableIndexPrefix(t *testing.T) {
	if GenTableIndexPrefix(2, 1) != "7480000000000000FF025F698000000000FF0000010000000000FA" {
		t.Fatalf("error TestGenTableIndexPrefix")
	}
}
func TestAppendTableRecordPrefix(t *testing.T) {
	byteA := []byte{97, 97, 97, 116, 128, 0, 0, 0, 0, 0, 0, 1, 95, 114}
	if string(appendTableRecordPrefix([]byte("aaa"), 1)) != string(byteA) {
		t.Fatalf("error TestAppendTableRecordPrefix")
	}
}
func TestAppendTableIndexPrefix(t *testing.T) {
	byteA := []byte{97, 97, 97, 116, 128, 0, 0, 0, 0, 0, 0, 1, 95, 105}
	if string(appendTableIndexPrefix([]byte("aaa"), 1)) != string(byteA) {
		t.Fatalf("error TestAppendTableIndexPrefix")
	}
}
