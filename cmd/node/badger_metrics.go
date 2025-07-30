package main

import (
	"expvar"
	"strconv"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/prometheus/client_golang/prometheus"
)

type BadgerMetricsCollector struct {
	descs   map[string]*prometheus.Desc
	metrics map[string]string
	logger  logger.Logger
}

type metricWithValue struct {
	Value  float64
	Labels map[string]string
}

const (
	gauge   = "Gauge"
	counter = "Counter"
)

// NewBadgerMetricsCollector creates a new BadgerMetricsCollector
// It initializes the collector with predefined metrics that are commonly used in BadgerDB monitoring.
// ref: https://github.com/hypermodeinc/badger/blob/main/y/metrics.go
func NewBadgerMetricsCollector(log logger.Logger) *BadgerMetricsCollector {
	descs := map[string]*prometheus.Desc{
		"badger_compaction_current_num_lsm": prometheus.NewDesc(
			"badger_lsm_compaction_current_num_total",
			"Number of LSM compactions currently running",
			nil, nil,
		),
		"badger_get_num_lsm": prometheus.NewDesc(
			"badger_lsm_get_num_total",
			"Number of LSM gets",
			[]string{"level"}, nil,
		),
		"badger_get_num_memtable": prometheus.NewDesc(
			"badger_memtable_get_num_total",
			"Number of memtable gets",
			nil, nil,
		),
		"badger_get_num_user": prometheus.NewDesc(
			"badger_user_get_num_total",
			"Number of user gets",
			nil, nil,
		),
		"badger_get_with_result_num_user": prometheus.NewDesc(
			"badger_user_get_with_result_num_total",
			"Number of user gets with results",
			nil, nil,
		),
		"badger_hit_num_lsm_bloom_filter": prometheus.NewDesc(
			"badger_hit_num_lsm_bloom_filter_total",
			"Number of LSM bloom filter hits",
			[]string{"level"}, nil,
		),
		"badger_iterator_num_user": prometheus.NewDesc(
			"badger_iterator_num_user_total",
			"Number of user iterators",
			nil, nil,
		),
		"badger_put_num_user": prometheus.NewDesc(
			"badger_user_put_num_total",
			"Number of user puts",
			nil, nil,
		),
		"badger_read_bytes_lsm": prometheus.NewDesc(
			"badger_lsm_read_bytes",
			"Bytes read from LSM",
			nil, nil,
		),
		"badger_size_bytes_lsm": prometheus.NewDesc(
			"badger_lsm_size_bytes",
			"Size of LSM in bytes",
			[]string{"path"}, nil, // With labels if needed
		),
		"badger_write_bytes_compaction": prometheus.NewDesc(
			"badger_compaction_write_bytes",
			"Bytes written during compaction",
			[]string{"level"}, nil, // With labels
		),
		"badger_write_bytes_l0": prometheus.NewDesc(
			"badger_l0_write_bytes",
			"Bytes written to L0",
			nil, nil,
		),
		"badger_write_bytes_user": prometheus.NewDesc(
			"badger_user_write_bytes",
			"Bytes written by user",
			nil, nil,
		),
		"badger_write_pending_num_memtable": prometheus.NewDesc(
			"badger_pending_num_memtable_write_total",
			"Number of pending writes in memtable",
			[]string{"path"}, nil, // With labels
		),
		"badger_size_bytes_vlog": prometheus.NewDesc(
			"badger_vlog_size_bytes",
			"Size of value log",
			[]string{"path"}, nil,
		),
		"badger_read_num_vlog": prometheus.NewDesc(
			"badger_read_num_vlog_total",
			"cumulative number of reads from vlog",
			nil, nil,
		),
		"badger_write_num_vlog": prometheus.NewDesc(
			"badger_write_num_vlog_total",
			"cumulative number of writes to vlog",
			nil, nil,
		),
		"badger_read_bytes_vlog": prometheus.NewDesc(
			"badger_read_vlog_bytes",
			"cumulative number of bytes read from vlog",
			nil, nil,
		),
		"badger_write_bytes_vlog": prometheus.NewDesc(
			"badger_write_vlog_bytes",
			"cumulative number of bytes written to vlog",
			nil, nil,
		),
	}
	metrics := map[string]string{
		"badger_read_num_vlog":              counter,
		"badger_write_num_vlog":             counter,
		"badger_read_bytes_vlog":            counter,
		"badger_write_bytes_vlog":           counter,
		"badger_read_bytes_lsm":             counter,
		"badger_write_bytes_l0":             counter,
		"badger_write_bytes_compaction":     counter,
		"badger_get_num_lsm":                counter,
		"badger_get_num_memtable":           counter,
		"badger_hit_num_lsm_bloom_filter":   counter,
		"badger_get_num_user":               counter,
		"badger_put_num_user":               counter,
		"badger_write_bytes_user":           counter,
		"badger_get_with_result_num_user":   counter,
		"badger_iterator_num_user":          counter,
		"badger_size_bytes_lsm":             gauge,
		"badger_size_bytes_vlog":            gauge,
		"badger_write_pending_num_memtable": gauge,
		"badger_compaction_current_num_lsm": counter,
	}
	return &BadgerMetricsCollector{
		descs:   descs,
		metrics: metrics,
		logger:  log,
	}
}

func (c *BadgerMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.descs {
		ch <- desc
	}
}

func (c *BadgerMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	// For each metric, you would get the value from expvar or another source
	// and create the appropriate metric type
	for name, desc := range c.descs {
		// get value from expvar
		metrics := c.getExpvarValueWithLabels(name)
		for _, m := range metrics {
			var labelValues []string
			if m.Labels != nil && m.Labels["key"] != "" {
				labelValues = append(labelValues, m.Labels["key"])
			}
			switch c.metrics[name] {
			case counter:
				// treat as Counter
				ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, m.Value, labelValues...)
			case gauge:
				// treat as Gauge
				ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, m.Value, labelValues...)
			default:
				// default to Gauge
				c.logger.Warnn("unknown metric type", logger.NewStringField("metricName", name))
				ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, m.Value, labelValues...)
			}
		}
	}
}

// If you need to handle map-type metrics like badger_size_bytes_lsm
func (c *BadgerMetricsCollector) getExpvarValueWithLabels(name string) []metricWithValue {
	v := expvar.Get(name)
	if v == nil {
		return nil
	}

	var result []metricWithValue

	switch val := v.(type) {
	case *expvar.Int:
		result = append(result, metricWithValue{Value: float64(val.Value())})
	case *expvar.Float:
		result = append(result, metricWithValue{Value: val.Value()})
	case *expvar.Map:
		val.Do(func(kv expvar.KeyValue) {
			var value float64
			if intVal, ok := kv.Value.(*expvar.Int); ok {
				value = float64(intVal.Value())
			} else if floatVal, ok := kv.Value.(*expvar.Float); ok {
				value = floatVal.Value()
			} else if f, err := strconv.ParseFloat(kv.Value.String(), 64); err == nil {
				value = f
			}

			result = append(result, metricWithValue{
				Value:  value,
				Labels: map[string]string{"key": kv.Key},
			})
		})
	default:
		c.logger.Warnn("unknown dtype for value", logger.NewStringField("metricName", name))
		if f, err := strconv.ParseFloat(v.String(), 64); err == nil {
			result = append(result, metricWithValue{Value: f})
		}
	}

	return result
}
