package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
)

type regionsInfo struct {
	Regions []*regionInfo `json:"regions"`
}

type dbInfo struct {
	Name struct {
		O string `json:"O"`
		L string `json:"L"`
	} `json:"db_name"`
	State int `json:"state"`
}

type tableInfo struct {
	ID   int64 `json:"id"`
	Name struct {
		O string `json:"O"`
		L string `json:"L"`
	} `json:"name"`
	Indices []struct {
		ID   int64 `json:"id"`
		Name struct {
			O string `json:"O"`
			L string `json:"L"`
		} `json:"idx_name"`
	} `json:"index_info"`
}

func request(addr string, uri string, v interface{}) {
	resp, err := http.Get(fmt.Sprintf("%s/%s", addr, uri))
	perr(err)
	r, err := ioutil.ReadAll(resp.Body)
	perr(err)
	err = resp.Body.Close()
	perr(err)
	err = json.Unmarshal(r, v)
	perr(err)
}

func regionRequest(key []byte, limit uint64) regionsInfo {
	uri := fmt.Sprintf("pd/api/v1/regions/key?key=%s&limit=%d", url.QueryEscape(string(key)), limit)
	var info regionsInfo
	request(*pdAddr, uri, &info)
	return info
}

func dbRequest(limit uint64) []*dbInfo {
	var dbInfos = make([]*dbInfo, limit)
	request(*tidbAddr, "schema", &dbInfos)
	return dbInfos
}

func tableRequest(limit uint64, s string) []*tableInfo {
	var tableInfos = make([]*tableInfo, limit)
	uri := fmt.Sprintf("schema/%s", s)
	request(*tidbAddr, uri, &tableInfos)
	return tableInfos
}
