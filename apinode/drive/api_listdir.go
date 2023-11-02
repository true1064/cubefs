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
	"container/list"
	"context"
	"fmt"
	"math"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
	"github.com/cubefs/cubefs/util"
)

type DirProperty struct {
	ID         uint64            `json:"fileId"`
	Type       string            `json:"type"`
	Properties map[string]string `json:"properties"`
}

type ListDirResult struct {
	DirProperty
	NextMarker string     `json:"nextMarker"`
	Files      []FileInfo `json:"files"`
}

type ArgsListAll struct {
	Path   FilePath `json:"path"`
	Limit  int      `json:"limit,omitempty"`
	Marker string   `json:"marker,omitempty"`
}

type ListAllResult struct {
	NextMarker string     `json:"nextMarker"`
	Files      []FileInfo `json:"files"`
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
	err := &sdk.Error{
		Status: sdk.ErrInvalidPath.Status,
		Code:   sdk.ErrInvalidPath.Code,
	}
	if !ok {
		err.Message = fmt.Sprintf("invalid filter[%s]", builder)
		return err
	}
	if builder.key == "type" && builder.value != typeFile && builder.value != typeFolder {
		err.Message = fmt.Sprintf("invalid filter[%s], type value is neither file nor folder", builder)
		return err
	}
	for _, op := range ops {
		if builder.operation == op {
			if builder.operation == opContains {
				re, e := regexp.Compile(builder.value)
				if e != nil {
					err.Message = fmt.Sprintf("invalid filter[%s], regexp.Compile error: %v", builder, e)
					return err
				}
				builder.re = re
			}
			return nil
		}
	}
	err.Message = fmt.Sprintf("invalid filter[%s]", builder)
	return err
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
			return nil, &sdk.Error{
				Status:  sdk.ErrInvalidPath.Status,
				Code:    sdk.ErrInvalidPath.Code,
				Message: fmt.Sprintf("invalid filter=%s", value),
			}
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
			if builder.match(k) {
				return true
			}
		}
		return false
	case "propertyValue":
		for _, v := range f.Properties {
			if builder.match(v) {
				return true
			}
		}
		return false
	}
	return false
}

func (d *DriveNode) handleListDir(c *rpc.Context) {
	args := new(ArgsListDir)
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args), args.Path.Clean()) {
		return
	}

	path, marker, limit := args.Path.String(), args.Marker, args.Limit

	uid := d.userID(c)
	var (
		pathIno Inode
		fileID  uint64
	)
	// 1. get user route info
	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if err != nil {
		span.Errorf("Failed to get volume: %v", err)
		d.respError(c, err)
		return
	}
	root := ur.RootFileID

	builders := []filterBuilder{}
	if path == "/" {
		pathIno = root
	} else {
		// 2. lookup the inode of dir
		dirInodeInfo, err := d.lookup(ctx, vol, root, path)
		if err != nil {
			span.Errorf("lookup path=%s error: %v", path, err)
			d.respError(c, err)
			return
		}
		if !dirInodeInfo.IsDir() {
			span.Errorf("path=%s is not a directory", path)
			d.respError(c, sdk.ErrNotDir)
			return
		}
		pathIno = Inode(dirInodeInfo.Inode)
		fileID = dirInodeInfo.FileId
	}

	if args.Filter != "" {
		bs, err := makeFilterBuilders(args.Filter)
		if err != nil {
			span.Errorf("makeFilterBuilders error: %v, path=%s, filter=%s", err, path, args.Filter)
			d.respError(c, &sdk.Error{Status: sdk.ErrBadRequest.Status, Code: sdk.ErrBadRequest.Code, Message: err.Error()})
			return
		}
		builders = append(builders, bs...)
	}

	res := ListDirResult{
		DirProperty: DirProperty{
			ID:         fileID,
			Type:       typeFolder,
			Properties: make(map[string]string),
		},
		Files: []FileInfo{},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	// lookup filePath's inode concurrency
	go func() {
		defer wg.Done()
		res.Properties, _ = vol.GetXAttrMap(ctx, pathIno.Uint64())
	}()

	if limit < 0 {
		limit = math.MaxUint32
	}
	if limit > math.MaxUint32 {
		limit = math.MaxUint32
	}
	isLast := false
	maxPerLimit := 10000 - 1

	for len(res.Files) < limit && !isLast {
		remains := limit - len(res.Files)
		for i := 0; i < remains && !isLast; i += maxPerLimit {
			n := maxPerLimit + 1
			if i+maxPerLimit > remains {
				n = remains - i + 1
			}
			fileInfo, err := d.listDir(ctx, pathIno.Uint64(), vol, marker, uint32(n))
			if err != nil {
				span.Errorf("list dir error: %v, path=%s", err, path)
				wg.Wait()
				d.respError(c, err)
				return
			}

			if len(fileInfo) < int(n) { // already at the end
				isLast = true
				marker = "" // clear marker
			} else {
				marker = fileInfo[len(fileInfo)-1].Name
				fileInfo = fileInfo[:len(fileInfo)-1]
			}

			if len(builders) > 0 {
				for j := 0; j < len(fileInfo); j++ {
					match := true
					for _, builder := range builders { // match all condition
						if !builder.matchFileInfo(&fileInfo[j]) {
							match = false
							break
						}
					}
					if match {
						res.Files = append(res.Files, fileInfo[j])
					}
				}
			} else {
				res.Files = append(res.Files, fileInfo...)
			}
		}
	}

	wg.Wait()
	res.NextMarker = marker
	if len(res.Files) > limit {
		res.Files = res.Files[:limit]
	}
	d.respData(c, res)
}

func (d *DriveNode) listDir(ctx context.Context, ino uint64, vol sdk.IVolume, marker string, limit uint32) (files []FileInfo, err error) {
	// invoke list interface to list files in path
	dirInfos, err := vol.Readdir(ctx, ino, marker, limit)
	if err != nil {
		return nil, err
	}

	n := len(dirInfos)
	inodes := make([]uint64, n)
	for i := 0; i < n; i++ {
		inodes[i] = dirInfos[i].Inode
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
	res = make(map[uint64]result)
	defer pool.Close()
	wg.Add(n)
	for i := 0; i < n; i++ {
		typ := typeFile
		if dirInfos[i].IsDir() {
			typ = typeFolder
		}
		ino := dirInfos[i].Inode
		fileID := dirInfos[i].FileId

		files = append(files, FileInfo{
			ID:         dirInfos[i].FileId,
			Ino:        ino,
			Name:       dirInfos[i].Name,
			Type:       typ,
			Size:       int64(inoInfo[i].Size),
			Ctime:      inoInfo[i].CreateTime.Unix(),
			Mtime:      inoInfo[i].ModifyTime.Unix(),
			Atime:      inoInfo[i].AccessTime.Unix(),
			Properties: make(map[string]string),
		})

		pool.Run(func() {
			defer wg.Done()
			properties, err := vol.GetXAttrMap(ctx, ino)
			mu.Lock()
			res[fileID] = result{properties, err}
			mu.Unlock()
		})
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		r := res[files[i].ID]
		if r.err != nil {
			return nil, r.err
		}
		if r.properties != nil {
			files[i].Properties = r.properties
		}
	}
	sort.Sort(FileInfoSlice(files))
	//
	return
}

type stackElement struct {
	ino    uint64
	name   string
	marker string
}

func getDirList(ctx context.Context, vol sdk.IVolume, ino uint64, marker string) (*list.List, *sdk.DirInfo, error) {
	var info *sdk.DirInfo
	stack := list.New()
	curMarker := ""
	stack.PushBack(&stackElement{ino, "", curMarker})
	dirs := strings.Split(marker, "/")
	for i, dir := range dirs {
		if dir == "" || dir == "." {
			continue
		}
		dirInfo, err := vol.Lookup(ctx, ino, dir)
		if err == sdk.ErrNotFound {
			break
		} else if err != nil {
			return nil, nil, err
		}

		fileType := typeFile
		curMarker = filepath.Join(curMarker, dirInfo.Name)
		if dirInfo.IsDir() {
			fileType = typeFolder
			stack.PushBack(&stackElement{dirInfo.Inode, dirInfo.Name, curMarker})
		}
		ino = dirInfo.Inode
		if i == len(dirs)-1 {
			info = dirInfo
		}
		if fileType == typeFile {
			break
		}
	}
	return stack, info, nil
}

func recursiveScan(ctx context.Context, vol sdk.IVolume, stack *list.List, marker string, limit int, result *ListAllResult) error {
	for stack.Len() > 0 {
		elem := stack.Back()
		e := elem.Value.(*stackElement)

		dirInfos, err := vol.ReadDirAll(ctx, e.ino)
		if err != nil {
			return err
		}
		needPop := true
		for _, dirInfo := range dirInfos {
			if filepath.Join(e.marker, dirInfo.Name) <= marker {
				continue
			}

			fileType := typeFile
			if dirInfo.IsDir() {
				fileType = typeFolder
			}

			marker = filepath.Join(e.marker, dirInfo.Name)

			result.Files = append(result.Files, FileInfo{
				ID:   dirInfo.FileId,
				Ino:  dirInfo.Inode,
				Name: marker,
				Type: fileType,
			})
			if len(result.Files) >= limit {
				return nil
			}
			if fileType == typeFolder {
				stack.PushBack(&stackElement{dirInfo.Inode, dirInfo.Name, marker})
				needPop = false
				break
			}
		}
		if needPop {
			stack.Remove(elem)
		}
	}
	return nil
}

func (d *DriveNode) handleListAll(c *rpc.Context) {
	args := new(ArgsListAll)
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args), args.Path.Clean()) {
		return
	}

	path := args.Path.String()
	marker := filepath.Clean(args.Marker)
	limit := args.Limit
	if limit <= 0 || limit > 10000 {
		limit = 10000
	}
	limit += 1

	uid := d.userID(c)
	var pathIno Inode

	// 1. get user route info
	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if err != nil {
		span.Errorf("Failed to get volume: %v", err)
		d.respError(c, err)
		return
	}
	root := ur.RootFileID

	if path == "/" {
		pathIno = root
	} else {
		// 2. lookup the inode of dir
		dirInodeInfo, errx := d.lookup(ctx, vol, root, path)
		if errx != nil {
			span.Errorf("lookup path=%s error: %v", path, errx)
			d.respError(c, errx)
			return
		}
		if !dirInodeInfo.IsDir() {
			span.Errorf("path=%s is not a directory", path)
			d.respError(c, sdk.ErrNotDir)
			return
		}
		pathIno = Inode(dirInodeInfo.Inode)
	}

	result := ListAllResult{}

	stack, dirInfo, err := getDirList(ctx, vol, pathIno.Uint64(), marker)
	if err != nil {
		d.respError(c, err)
		return
	}

	if dirInfo != nil {
		fileType := typeFile
		if dirInfo.IsDir() {
			fileType = typeFolder
		}
		result.Files = append(result.Files, FileInfo{
			ID:   dirInfo.FileId,
			Ino:  dirInfo.Inode,
			Name: marker,
			Type: fileType,
		})
	}

	if err = recursiveScan(ctx, vol, stack, marker, limit, &result); err != nil {
		d.respError(c, err)
		return
	}

	n := len(result.Files)
	pool := taskpool.New(util.Min(n, maxTaskPoolSize), n)
	var wg sync.WaitGroup
	var errValue atomic.Value
	wg.Add(n)
	for i, file := range result.Files {
		idx := i
		ino := file.Ino
		pool.Run(func() {
			defer wg.Done()
			if errValue.Load() != nil {
				return
			}
			inoInfo, err := vol.GetInode(ctx, ino)
			if err != nil {
				span.Errorf("get inode error: %v, name: %s, inode: %s", err, result.Files[idx].Name, ino)
				errValue.Store(err)
				return
			}
			properties, err := vol.GetXAttrMap(ctx, ino)
			if err != nil {
				span.Errorf("get xattr error: %v, name: %s, inode: %d", err, result.Files[idx].Name, ino)
				errValue.Store(err)
				return
			}
			if properties == nil {
				properties = make(map[string]string)
			}
			result.Files[idx].Size = int64(inoInfo.Size)
			result.Files[idx].Ctime = inoInfo.CreateTime.Unix()
			result.Files[idx].Mtime = inoInfo.ModifyTime.Unix()
			result.Files[idx].Atime = inoInfo.AccessTime.Unix()
			result.Files[idx].Properties = properties
		})
	}
	wg.Wait()
	pool.Close()
	if v := errValue.Load(); v != nil {
		d.respError(c, v.(error))
		return
	}
	if limit <= len(result.Files) {
		result.NextMarker = result.Files[limit-1].Name
		result.Files = result.Files[0 : limit-1]
	}
	d.respData(c, result)
}
