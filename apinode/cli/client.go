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

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/cubefs/cubefs/apinode/drive"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

const (
	post = http.MethodPost
	put  = http.MethodPut
	get  = http.MethodGet
	del  = http.MethodDelete

	_pmp = "/v1/files/multipart"
)

type readCloser struct {
	io.Reader
	io.Closer
}

var cli = &client{Client: rpc.NewClient(&rpc.Config{})}

type client struct {
	rpc.Client
}

func setHeaders(req *http.Request, material string, meta []string) error {
	req.Header.Set("x-cfa-service", "drive")
	req.Header.Set(drive.HeaderUserID, user)
	if len(pass) > 0 {
		req.Header.Set(drive.HeaderCipherMaterial, "1"+pass)
	}
	for i := 0; i < len(meta); i += 2 {
		k, err := encoder.Encrypt(meta[i], false)
		if err != nil {
			return err
		}
		v, err := encoder.Encrypt(meta[i+1], false)
		if err != nil {
			return err
		}
		req.Header.Set(drive.EncodeMetaHeader(k), drive.EncodeMeta(v))
	}
	return nil
}

func (c *client) request(method string, uri string, body io.Reader, meta ...string) error {
	return c.requestWith(method, uri, body, nil, meta...)
}

func (c *client) requestWith(method string, uri string, body io.Reader, ret interface{}, meta ...string) error {
	return c.requestWithHeader(method, uri, body, nil, ret, meta...)
}

func (c *client) requestWithHeader(method string, uri string, body io.Reader, headers map[string]string,
	ret interface{}, meta ...string,
) error {
	var material string
	if body != nil {
		body, material = requester(body)
	} else {
		_, material = requester(io.MultiReader())
	}
	req, err := http.NewRequest(method, host+uri, body)
	if err != nil {
		return err
	}
	if err = setHeaders(req, material, meta); err != nil {
		return err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.Do(context.Background(), req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 == 2 {
		resp.Body = readCloser{Reader: responser(resp.Body, resp.Header.Get(drive.HeaderCipherMaterial)), Closer: resp.Body}
	}
	return newStatusError(rpc.ParseData(resp, ret), resp.Header.Get("x-cfa-trace-id"))
}

type statusError struct {
	request string
	err     error
}

func (se *statusError) Error() string {
	if e, ok := se.err.(rpc.HTTPError); ok {
		return fmt.Sprintf("{request:[%s] status:[%d] code:[%s] error:[%s]}",
			se.request, e.StatusCode(), e.ErrorCode(), e.Error())
	}
	return se.err.Error()
}

func newStatusError(err error, request string) error {
	if err == nil {
		return nil
	}
	return &statusError{request: request, err: err}
}

func (c *client) MetaSet(path string, meta ...string) error {
	return c.request(post, genURI("/v1/files/properties", "path", path), nil, meta...)
}

func (c *client) MetaDel(path string, keys ...string) error {
	meta := make([]string, 0, 2*len(keys))
	for _, key := range keys {
		meta = append(meta, key, "1")
	}
	return c.request(del, genURI("/v1/files/properties", "path", path), nil, meta...)
}

func (c *client) MetaGet(path string) (r drive.GetPropertiesResult, err error) {
	err = c.requestWith(get, genURI("/v1/files/properties", "path", path), nil, &r)
	return
}

func (c *client) ListDir(path, marker, limit, filter string) (r drive.ListDirResult, err error) {
	err = c.requestWith(get, genURI("/v1/files",
		"path", path, "marker", marker, "limit", limit, "filter", filter), nil, &r)
	return
}

func (c *client) MkDir(path string, recursive bool) error {
	return c.request(post, genURI("/v1/files/mkdir", "path", path, "recursive", recursive), nil)
}

func (c *client) FileUpload(path string, fileID uint64, body io.Reader, meta ...string) (r drive.FileInfo, err error) {
	err = c.requestWith(put, genURI("/v1/files/upload", "path", path, "fileId", fileID), body, &r, meta...)
	return
}

func (c *client) FileWrite(path string, fileID uint64, from, to int, body io.Reader, size int) error {
	return c.requestWithHeader(put, genURI("/v1/files/content", "path", path, "fileId", fileID), body,
		map[string]string{"Range": getRange(from, to), rpc.HeaderContentLength: fmt.Sprint(size)}, nil)
}

func (c *client) FileVerify(path string, from, to int, checksum map[string]string) (err error) {
	header := make(map[string]string)
	if from >= 0 || to >= 0 {
		header["Range"] = getRange(from, to)
	}
	for k, v := range checksum {
		header[drive.ChecksumPrefix+k] = v
	}
	return c.requestWithHeader(get, genURI("/v1/files/verify", "path", path), nil, header, nil)
}

func (c *client) FileDownload(path string, from, to int, w io.Writer) (err error) {
	req, err := http.NewRequest(get, host+genURI("/v1/files/content", "path", path), nil)
	if err != nil {
		return
	}
	_, material := requester(io.MultiReader())
	setHeaders(req, material, nil)
	if from >= 0 || to >= 0 {
		req.Header.Set("Range", getRange(from, to))
	}

	resp, err := c.Do(context.Background(), req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 || resp.StatusCode == 206 {
		if _, err = io.Copy(w, responser(resp.Body, resp.Header.Get(drive.HeaderCipherMaterial))); err == io.EOF {
			err = nil
		}
		return
	}
	err = newStatusError(rpc.ParseData(resp, nil), resp.Header.Get("x-cfa-trace-id"))
	return
}

func (c *client) FileCopy(src, dst string, meta bool) error {
	return c.request(post, genURI("/v1/files/copy", "src", src, "dst", dst, "meta", meta), nil)
}

func (c *client) FileRename(src, dst string) error {
	return c.request(post, genURI("/v1/files/rename", "src", src, "dst", dst), nil)
}

func (c *client) FileDelete(path string, recursive bool) error {
	return c.request(del, genURI("/v1/files", "path", path, "recursive", recursive), nil)
}

func (c *client) FileBatchDelete(paths []string) (r drive.BatchDeleteResult, err error) {
	files := make([]drive.FilePath, 0, len(paths))
	for idx := range paths {
		files = append(files, drive.FilePath(paths[idx]))
	}
	b, err := json.Marshal(drive.ArgsBatchDelete{Paths: files})
	if err != nil {
		panic(err)
	}
	err = c.requestWith(del, genURI("/v1/files/batch"), bytes.NewReader(b), &r)
	return
}

func (c *client) SnapshotCreate(path, ver string) error {
	return c.request(post, genURI("/v1/snapshot", "path", path, "ver", ver), nil)
}

func (c *client) SnapshotDelete(path, ver string) error {
	return c.request(del, genURI("/v1/snapshot", "path", path, "ver", ver), nil)
}

func (c *client) MPInit(path, fileID string, meta ...string) (r drive.RespMPuploads, err error) {
	err = c.requestWith(post, genURI(_pmp, "path", path, "fileId", fileID), nil, &r, meta...)
	return
}

func (c *client) MPComplete(path, uploadID string, body io.Reader) (r drive.FileInfo, err error) {
	err = c.requestWith(post, genURI(_pmp, "path", path, "uploadId", uploadID), body, &r)
	return
}

func (c *client) MPPart(path, uploadID, partNumber string, body io.Reader) (r drive.MPPart, err error) {
	err = c.requestWith(put, genURI(_pmp, "path", path, "uploadId", uploadID, "partNumber", partNumber), body, &r)
	return
}

func (c *client) MPList(path, uploadID, marker, limit string) (r drive.RespMPList, err error) {
	err = c.requestWith(get, genURI(_pmp, "path", path, "uploadId", uploadID, "marker", marker, "limit", limit), nil, &r)
	return
}

func (c *client) MPAbort(path, uploadID string) error {
	return c.request(del, genURI(_pmp, "path", path, "uploadId", uploadID), nil)
}

func genURI(uri string, queries ...interface{}) string {
	if len(queries)%2 == 1 {
		queries = append(queries, "")
	}
	q := make(url.Values)
	for i := 0; i < len(queries); i += 2 {
		v, err := encoder.Encrypt(fmt.Sprint(queries[i+1]), true)
		if err != nil {
			panic(err)
		}
		q.Set(fmt.Sprint(queries[i]), v)
	}
	if len(q) > 0 {
		return uri + "?" + q.Encode()
	}
	return uri
}

func getRange(from, to int) string {
	if from >= 0 {
		if to >= 0 {
			return fmt.Sprintf("bytes=%d-%d", from, to)
		}
		return fmt.Sprintf("bytes=%d-", from)
	} else if to >= 0 {
		return fmt.Sprintf("bytes=-%d", to)
	}
	return "bytes="
}
