package main

import (
	"reflect"
	"testing"
)

func TestScanRegions(t *testing.T) {
	regions := scanRegions()
	newRegions := scanRegions()
	if regions == nil || len(regions) == 0 || newRegions == nil || len(newRegions) == 0 {
		t.Fatalf("error scan regions")
	}
	if !reflect.DeepEqual(regions, newRegions) {
		t.Fatalf("two scan not the same, before: \n%v\n, after: \n%v\n", regions, newRegions)
	}
}
