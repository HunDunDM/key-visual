package main

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/rs/cors"
	"net/http"
	"time"
)

var (
	//服务器监听的 IP 地址和端口号
	addr = flag.String("addr", "0.0.0.0:8000", "Listening address")
	//PD 服务器地址
	pdAddr = flag.String("pd", "http://172.16.4.191:8010", "PD address")
	//TiDB服务器地址
	tidbAddr = flag.String("tidb", "http://172.16.4.191:10080", "TiDB Address")
	//interval
	interval  = flag.Duration("I", 10*time.Second, "Interval to collect metrics")
)

func handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-type", "application/json")
	startKey := r.FormValue("startkey")
	endKey := r.FormValue("endkey")
	start := r.FormValue("starttime")
	end := r.FormValue("endtime")
	endTime := time.Now()
	startTime := endTime.Add(-60 * time.Minute)
	// tag参数表示哪种数据指标
	tag := r.FormValue("tag")
	// mode参数表示数据统计的模式，如最大值、平均值
	mode := r.FormValue("mode")

	if start != "" {
		if d, err := time.ParseDuration(start); err == nil {
			startTime = endTime.Add(d)
		}
	}
	if end != "" {
		if d, err := time.ParseDuration(end); err == nil {
			endTime = endTime.Add(d)
		}
	}
	if endKey == "" {
		endKey = "~" //\126
	}
	matrix := generateHeatmap(startTime, endTime, startKey, endKey, tag, mode)
	data, _ := json.Marshal(matrix)
	_, err := w.Write(data)
	perr(err)
}

func updateStat(ctx context.Context) {
	//ticker := time.NewTicker(time.Minute)
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			regions := scanRegions()
			globalRegionStore.Append(regions)
			updateTables()
		}
	}
}

func main() {
	flag.Parse()
	// 循环更新数据
	go updateStat(context.Background())
	mux := http.NewServeMux()
	mux.HandleFunc("/heatmaps", handler)

	// cors.Default() setup the middleware with default options being
	// all origins accepted with simple methods (GET, POST). See
	// documentation below for more options.
	handler := cors.Default().Handler(mux)

	_ = http.ListenAndServe(*addr, handler)

	globalRegionStore.Close()
	// 关闭tableDb
	tables.Close()
}
