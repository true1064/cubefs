// Copyright 2023 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package apinode

import (
	"os"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "cfa"
	subsystem = "drive"
)

var (
	hostname    = getHostname()
	labels      = []string{"region", "cluster", "idc", "handle", "method", "code"}
	constLabels = map[string]string{"host": hostname}
)

var handleCodeMetric = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace:   namespace,
		Subsystem:   subsystem,
		Name:        "handlecode",
		Help:        "handle code counter",
		ConstLabels: constLabels,
	}, labels[:],
)

func init() {
	prometheus.MustRegister(handleCodeMetric)
}

func getHostname() string {
	hostname, _ := os.Hostname()
	return hostname
}

func handleCounter(handle, method string, code int) {
	handleCodeMetric.WithLabelValues("todo", "todo", "todo", handle, method, strconv.Itoa(code)).Inc()
}
