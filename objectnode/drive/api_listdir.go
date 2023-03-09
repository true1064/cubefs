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
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/util/log"
)

type filterBuilder struct {
	key       string
	operation string
	value     string
}

const (
	opContains = "contains"
	opEqual    = "="
	opNotEqual = "!="
)

var filterKeyMap = map[string][]string{
	"name":          {opContains, opEqual, opNotEqual},
	"type":          {opEqual},
	"propertyKey":   {opContains, opEqual, opNotEqual},
	"propertyValue": {opContains, opEqual, opNotEqual},
}

func validFilterBuilder(builder filterBuilder) error {
	ops, ok := filterKeyMap[builder.key]
	if !ok {
		return fmt.Errorf("invalid filter[%v]", builder)
	}
	if builder.key == "type" && builder.value != "file" && builder.value != "folder" {
		return fmt.Errorf("invalid filter[%v], type value is neither file or folder", builder)
	}
	for _, op := range ops {
		if builder.operation == op {
			return nil
		}
	}
	return fmt.Errorf("invalid filter[%v]", builder)
}

func makeFilterBuilders(value string) ([]filterBuilder, error) {
	filters := strings.Split(value, ";")
	if len(filters) == 0 {
		return nil, nil
	}
	var builders []filterBuilder
	for _, s := range filters {
		f := strings.Split(s, " ")
		if len(f) != 3 {
			return nil, fmt.Errorf("invalid filter=%s", value)
		}
		builder := filterBuilder{f[0], f[1], f[2]}
		if err := validFilterBuilder(builder); err != nil {
			return nil, err
		}
		builders = append(builders, builder)
	}
	return builders, nil
}

func (builder *filterBuilder) match(v string) bool {
	switch builder.operation {
	case opContains:
		matched, _ := regexp.MatchString(builder.value, v)
		return matched
	case opEqual:
		return builder.value == v
	case opNotEqual:
		return builder.value != v
	}
	return false
}

func (d *DriveNode) handlerListDir(c *rpc.Context) {
	args := new(ArgsListDir)
	if err := c.ParseArgs(args); err != nil {
		c.RespondStatus(http.StatusBadRequest)
		return
	}

	/*
	   limit := vars["limit"]
	   mark := vars["mark"]
	   filter := vars["filter"]
	   owner := vars["owner"]
	*/

	path, typ := args.Path, args.Type

	if path == "" || typ == "" {
		log.LogErrorf("not found path or type in paraments")
		c.RespondStatus(http.StatusBadRequest)
		return
	}

	if typ != "folder" {
		log.LogErrorf("invalid param type=%s", typ)
		c.RespondStatus(http.StatusBadRequest)
		return
	}

	// 1. get user route info
	// 2. if has owner, we should list shared files
}

func (d *DriveNode) listDir(path string, mark string, limit int, filter string) (files []FileInfo, err error) {
	//invoke list interface to list files in path
	//
	return
}

func (d *DriveNode) listShareDir(path string, mark string, limit int, filter string, uid, owner string) (files []SharedFileInfo, err error) {
	return
}
