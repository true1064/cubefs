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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
)

type mockMaster struct{}

func (mockMaster) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.RequestURI() {
	case proto.AdminGetIP:
		w.Write([]byte(`{"data":{"Cluster":"test_server"}}`))
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func mockMasterServer() (string, func()) {
	testServer := httptest.NewServer(mockMaster{})
	return testServer.URL[len("http://"):], func() {
		testServer.Close()
	}
}

func TestServer(t *testing.T) {
	ma, cl := mockMasterServer()
	logdir := path.Join(os.TempDir(), "apinode", "testserver")
	defer func() {
		cl()
		os.RemoveAll(logdir)
	}()

	masterAddrs := "\"" + configMasterAddr + "\":\"" + ma + "\""
	kv := func(k, v string) string {
		return fmt.Sprintf("\"%s\": \"%s\"", k, v)
	}
	cases := [][]string{
		nil,
		{
			kv(configLogDir, logdir),
		},
		{
			kv(configLogDir, logdir),
			kv(configLogLevel, "warn"),
		},
		{
			kv(configLogDir, logdir),
			kv(configListen, "0"),
			kv(configLogLevel, "debug"),
		},
		{
			kv(configLogDir, logdir),
			kv(configListen, ":0"),
			kv(configLogLevel, "error"),
		},
	}
	for _, cs := range cases {
		s := NewServer().(*apiNode)
		cfg := config.LoadConfigString("{" + strings.Join(cs, ",") + "}")
		require.Error(t, s.loadConfig(cfg))
	}

	{
		kvs := []string{
			kv(configLogDir, logdir),
			kv(configListen, ":0"),
			kv(configLogLevel, "info"),
			masterAddrs,
		}
		s := NewServer()
		cfg := config.LoadConfigString("{" + strings.Join(kvs, ",") + "}")
		require.Error(t, s.Start(cfg))
	}
}
