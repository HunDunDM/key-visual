package main

import (
	"fmt"
	"testing"
)

func TestRequests_request(t *testing.T) {
	var infos regionsInfo
	uri := fmt.Sprintf("pd/api/v1/regions/key?key=%s&limit=%d", "", 1024)
	request(*pdAddr, uri, &infos)
	if infos.Regions == nil || len(infos.Regions) == 0 {
		t.Fatalf("error request regionInfo")
	}
	var dbInfos = make([]*dbInfo, 0)
	request(*tidbAddr, "schema", &dbInfos)
	if dbInfos == nil || len(dbInfos) == 0 {
		t.Fatalf("error request dbInfo")
	}
	for _, info := range dbInfos {
		if info.State == 0 {
			continue
		}
		var tableInfos = make([]*tableInfo, 0)
		uri := fmt.Sprintf("schema/%s", info.Name.O)
		request(*tidbAddr, uri, &tableInfos)
		if tableInfos == nil {
			t.Fatalf("error request tableInfo")
		}
	}
}
func TestRequests_dbRequest(t *testing.T) {
	dbInfos := dbRequest(0)
	if len(dbInfos) == 0 {
		t.Fatalf("error dbInfo")
	}
}
