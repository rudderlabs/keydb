package main

var defaultHistogramBuckets = []float64{
	0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
	300 /* 5 mins */, 600 /* 10 mins */, 1800, /* 30 mins */
}

var customBuckets = map[string][]float64{}
