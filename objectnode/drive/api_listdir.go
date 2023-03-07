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

package drive

import (
	"net/http"

	"github.com/cubefs/cubefs/util/log"
	"github.com/gorilla/mux"
)

func (d *DriveNode) listDirHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uid := r.Header.Get("X-UserID")
	if uid == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	path := vars["path"]
	typ := vars["type"]
	limit := vars["limit"]
	mark := vars["mark"]
	filter := vars["filter"]
	owner := vars["owner"]

	if path == "" || typ == "" {
		log.LogErrorf("not found path or type in paraments")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if typ != "folder" {
		log.LogErrorf("invalid param type=%s", typ)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// 1. get user route info
	// 2. 判断owner是否存在 决定是否列举共享目录

}

func (d *DriveNode) listDir(path string, mark string, limit int, filter string) {
}

func (d *DriveNode) listShareDir(path string, mark string, limit int, filter string, uid, owner string) {
}
