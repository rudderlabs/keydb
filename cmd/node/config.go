package main

const (
	serviceName = "keydb"
)

var defaultHistogramBuckets = []float64{
	0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
	300 /* 5 mins */, 600 /* 10 mins */, 1800, /* 30 mins */
}

var customBuckets = map[string][]float64{
	"keydb_gc_duration_seconds": {
		// 2ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, 20s, 30s, 1m, 5m, 10m, 30m
		0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60, 300, 600, 1800,
	},
	"keydb_grpc_req_latency_seconds": {
		// 1ms, 2ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, 20s, 30s
		0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30,
	},
	"keydb_req_latency_seconds": {
		// 1ms, 2ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, 20s
		0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20,
	},
	"keydb_keys_hashing_duration_seconds": {
		// 1 micro, 5 micro, 10 micro, 50 micro, 100 micro, 500 micro, 1ms, 5ms, 10ms
		0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01,
	},
	"keydb_grpc_cache_get_duration_seconds": {
		// 10microsecond, 25microsecond, 50microsecond, 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s
		0.00001, 0.00025, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
	},
	"keydb_grpc_cache_put_duration_seconds": {
		// 10microsecond, 25microsecond, 50microsecond, 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s
		0.00001, 0.00025, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
	},
	"keydb_download_snapshot_duration_seconds": {
		// 1ms, 2ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, 20s, 30s, 60s
		0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60,
	},
	"keydb_load_snapshot_to_disk_duration_seconds": {
		// 1ms, 2ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, 20s, 30s, 60s
		0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60,
	},
	"keydb_upload_snapshot_duration_seconds": {
		// 1ms, 2ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, 20s, 30s, 60s
		0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60,
	},
	"keydb_create_snapshot_duration_seconds": {
		// 1ms, 2ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, 20s, 30s, 60s
		0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60,
	},
}
