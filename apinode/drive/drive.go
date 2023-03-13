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
	"strconv"
)

const (
	headerRequestID = "x-cfa-request-id"
	headerUserID    = "x-cfa-user-id"
)

type FileInfo struct {
	ID         uint64            `json:"id"`
	Name       string            `json:"name"`
	Type       string            `json:"type"`
	Size       int64             `json:"size"`
	Ctime      int64             `json:"ctime"`
	Mtime      int64             `json:"mtime"`
	Atime      int64             `json:"atime"`
	Properties map[string]string `json:"properties"`
}

type SharedFileInfo struct {
	ID    uint64 `json:"id"`
	Path  string `json:"path"`
	Owner string `json:"owner"`
	Type  string `json:"type"`
	Size  int64  `json:"size"`
	Ctime int64  `json:"ctime"`
	Mtime int64  `json:"mtime"`
	Atime int64  `json:"atime"`
	Perm  string `json:"perm"` // only rd or rw
}

type UserID uint64

func (u UserID) String() string {
	return strconv.Itoa(int(u))
}

type ArgsListDir struct {
	Path string `json:"path"`
	Type string `json:"type"`
}

// DriveNode drive node.
type DriveNode struct {
	userRouteMgr
}
