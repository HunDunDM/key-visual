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
	// the IP address and port number that this server listen on
	addr = flag.String("addr", "0.0.0.0:8000", "Listening address")
	// PD Server address
	pdAddr = flag.String("pd", "http://172.16.4.191:8010", "PD address")
	// TiDB Server address
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
	// tag indicates the type of data request(e.g. read or write)
	tag := r.FormValue("tag")
	// mode indicates the mod of data statistics(e.g. max or average)
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
		endKey = "~" // \126, which is the biggest displayable character
	}
	matrix := GenerateHeatmap(startTime, endTime, startKey, endKey, tag, mode)
	data, _ := json.Marshal(matrix)
	_, err := w.Write(data)
	perr(err)
}

func updateStat(ctx context.Context) {
	// use ticker to get data at certain intervals
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			regions := ScanRegions()
			globalRegionStore.Append(regions)
			updateTables()
		}
	}
}

func main() {
	flag.Parse()
	// update data loop
	go updateStat(context.Background())
	mux := http.NewServeMux()
	mux.HandleFunc("/heatmaps", handler)

	// cors.Default() setup the middleware with default options being
	// all origins accepted with simple methods (GET, POST). See
	// documentation below for more options.
	handler := cors.Default().Handler(mux)

	_ = http.ListenAndServe(*addr, handler)
	// close the two levelDbs
	globalRegionStore.Close()
	tables.Close()
}
