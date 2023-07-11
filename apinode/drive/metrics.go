package drive

import "github.com/prometheus/client_golang/prometheus"

var processOplogFailedCount = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace:   "apinode",
	Subsystem:   "drive",
	Name:        "process_oplog_failed_count",
	Help:        "The failed number of oplog records",
	ConstLabels: make(map[string]string),
})

func init() {
	prometheus.MustRegister(processOplogFailedCount)
}
