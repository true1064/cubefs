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
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
	"github.com/cubefs/cubefs/util"
)

type ListDirResult struct {
	ID         uint64            `json:"id"`
	Type       string            `type:"type"`
	NextMarker string            `type:"nextMarker"`
	Properties map[string]string `type:"properties"`
	Files      []FileInfo        `type:"files"`
}

type filterBuilder struct {
	re        *regexp.Regexp
	key       string
	operation string
	value     string
}

const (
	opContains = "contains"
	opEqual    = "="
	opNotEqual = "!="
)

var usrFolderFilter = filterBuilder{
	re:        nil,
	key:       "name",
	operation: opNotEqual,
	value:     ".usr",
}

type FileInfoSlice []FileInfo

func (s FileInfoSlice) Len() int           { return len(s) }
func (s FileInfoSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s FileInfoSlice) Less(i, j int) bool { return s[i].Name < s[j].Name }

var filterKeyMap = map[string][]string{
	"name":          {opContains, opEqual, opNotEqual},
	"type":          {opEqual},
	"propertyKey":   {opContains, opEqual, opNotEqual},
	"propertyValue": {opContains, opEqual, opNotEqual},
}

func validFilterBuilder(builder *filterBuilder) error {
	ops, ok := filterKeyMap[builder.key]
	if !ok {
		return fmt.Errorf("invalid filter[%s]", builder)
	}
	if builder.key == "type" && builder.value != "file" && builder.value != "folder" {
		return fmt.Errorf("invalid filter[%s], type value is neither file nor folder", builder)
	}
	for _, op := range ops {
		if builder.operation == op {
			if builder.operation == opContains {
				re, err := regexp.Compile(builder.value)
				if err != nil {
					return fmt.Errorf("invalid filter[%s], regexp.Compile error: %v", builder, err)
				}
				builder.re = re
			}
			return nil
		}
	}
	return fmt.Errorf("invalid filter[%s]", builder)
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
		builder := filterBuilder{key: f[0], operation: f[1], value: f[2]}
		if err := validFilterBuilder(&builder); err != nil {
			return nil, err
		}
		builders = append(builders, builder)
	}
	return builders, nil
}

func (builder *filterBuilder) String() string {
	return fmt.Sprintf("{key: %s, op: %s, value: %s}", builder.key, builder.operation, builder.value)
}

func (builder *filterBuilder) match(v string) bool {
	switch builder.operation {
	case opContains:
		return builder.re.MatchString(v)
	case opEqual:
		return builder.value == v
	case opNotEqual:
		return builder.value != v
	}
	return false
}

func (builder *filterBuilder) matchFileInfo(f *FileInfo) bool {
	switch builder.key {
	case "name":
		return builder.match(f.Name)
	case "type":
		return builder.match(f.Type)
	case "propertyKey":
		for k := range f.Properties {
			if !builder.match(k) {
				delete(f.Properties, k)
			}
		}
		return true
	case "propertyValue":
		for k, v := range f.Properties {
			if !builder.match(v) {
				delete(f.Properties, k)
			}
		}
		return true
	}
	return false
}

func (d *DriveNode) handleListDir(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	args := new(ArgsListDir)
	if err := c.ParseArgs(args); err != nil {
		c.RespondStatus(http.StatusBadRequest)
		return
	}

	uid := d.userID(c)

	path, marker, limit := filepath.Clean(args.Path), args.Marker, args.Limit

	var (
		rootIno Inode
		pathIno Inode
		vol     sdk.IVolume
		err     error
	)
	// 1. get user route info
	rootIno, vol, err = d.getRootInoAndVolume(ctx, uid)
	if err != nil {
		span.Errorf("Failed to get volume: %v", err)
		c.RespondError(err)
		return
	}

	if path == "/" {
		pathIno = rootIno
	} else {
		// 2. lookup the inode of dir
		dirInodeInfo, err := d.lookup(ctx, vol, rootIno, path)
		if err != nil {
			span.Errorf("lookup path=%s error: %v", path, err)
			c.RespondError(err)
			return
		}
		if !dirInodeInfo.IsDir() {
			span.Errorf("path=%s is not a directory", path)
			c.RespondError(sdk.ErrNotDir)
			return
		}
		pathIno = Inode(dirInodeInfo.Inode)
	}

	var (
		res ListDirResult
		wg  sync.WaitGroup
	)

	res.ID = pathIno.Uint64()

	wg.Add(1)
	// lookup filePath's inode concurrency
	go func() {
		defer wg.Done()
		res.Properties, _ = vol.GetXAttrMap(ctx, res.ID)
	}()

	if limit > 0 {
		limit++ // get one more
	}
	getMore := true
	for getMore {
		fileInfo, err := d.listDir(ctx, pathIno.Uint64(), vol, marker, limit)
		if err != nil {
			span.Errorf("list dir error: %v, path=%s", err, path)
			c.RespondError(err)
			return
		}

		if limit < 0 || len(fileInfo) < limit { // already at the end of the list
			getMore = false
		}

		builders := []filterBuilder{}

		if args.Filter != "" {
			builders, err = makeFilterBuilders(args.Filter)
			if err != nil {
				span.Errorf("makeFilterBuilders error: %v, path=%s, filter=%s", err, path, args.Filter)
				c.RespondError(err)
				return
			}
		}
		if path == "/" { // exclude /.usr dir
			builders = append(builders, usrFolderFilter)
		}

		if len(builders) > 0 {
			for i := 0; i < len(fileInfo); i++ {
				for _, builder := range builders {
					if !builder.matchFileInfo(&fileInfo[i]) {
						continue
					}
					res.Files = append(res.Files, fileInfo[i])
				}
			}
		} else {
			res.Files = fileInfo
		}
		if getMore && len(res.Files) == limit {
			getMore = false
		}
	}

	wg.Wait()
	if limit > 0 && len(res.Files) == limit {
		res.NextMarker = res.Files[len(res.Files)-1].Name
		res.Files = res.Files[:len(res.Files)-1]
	}
	c.RespondJSON(res)
}

func (d *DriveNode) listDir(ctx context.Context, ino uint64, vol sdk.IVolume, marker string, limit int) (files []FileInfo, err error) {
	// invoke list interface to list files in path
	dirInfos, err := vol.Readdir(ctx, ino, marker, uint32(limit))
	if err != nil {
		return nil, err
	}

	n := len(dirInfos)
	inodes := make([]uint64, n)
	for i := 0; i < n; i++ {
		inodes = append(inodes, dirInfos[i].Inode)
	}
	inoInfo, err := vol.BatchGetInodes(ctx, inodes)
	if err != nil {
		return nil, err
	}

	pool := taskpool.New(util.Min(n, maxTaskPoolSize), n)
	type result struct {
		properties map[string]string
		err        error
	}

	var (
		res map[uint64]result
		wg  sync.WaitGroup
		mu  sync.Mutex
	)
	defer pool.Close()
	wg.Add(n)
	for i := 0; i < n; i++ {
		typ := "file"
		if dirInfos[i].IsDir() {
			typ = "folder"
		}
		ino := dirInfos[i].Inode

		files = append(files, FileInfo{
			ID:    dirInfos[i].Inode,
			Name:  dirInfos[i].Name,
			Type:  typ,
			Size:  int64(inoInfo[i].Size),
			Ctime: inoInfo[i].CreateTime.Unix(),
			Mtime: inoInfo[i].ModifyTime.Unix(),
			Atime: inoInfo[i].AccessTime.Unix(),
		})

		pool.Run(func() {
			defer wg.Done()
			properties, err := vol.GetXAttrMap(ctx, ino)
			mu.Lock()
			res[ino] = result{properties, err}
			mu.Unlock()
		})
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		r := res[files[i].ID]
		if r.err != nil {
			return nil, r.err
		}
		files[i].Properties = r.properties
	}
	sort.Sort(FileInfoSlice(files))
	//
	return
}
