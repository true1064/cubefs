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
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

func TestHandleFilesMakedir(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(queries ...string) rpc.HTTPError {
		url := genURL(server.URL, "/v1/files/mkdir", queries...)
		req, _ := http.NewRequest(http.MethodPost, url, nil)
		req.Header.Add(HeaderRequestID, "user_request_id")
		req.Header.Add(HeaderUserID, testUserID)
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		return resp2Error(resp)
	}

	{
		require.Equal(t, 400, doRequest("src", "/a").StatusCode())
		require.Equal(t, 400, doRequest("path", "a/b/../../..").StatusCode())
	}
	{
		node.TestGetUser(t, func() rpc.HTTPError { return doRequest("path", "/dira") }, testUserID)
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		require.Equal(t, e1.Status, doRequest("path", "/dira/dirb/").StatusCode())
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		require.Equal(t, 404, doRequest("path", "/dira/dirb/").StatusCode())
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound).Times(2)
		node.Volume.EXPECT().Mkdir(A, A, A).Return(&sdk.InodeInfo{Inode: node.GenInode()}, nil).Times(2)
		require.NoError(t, doRequest("path", "/dira/dirb/", "recursive", "1"))
	}
}

func TestHandleFilesDelete(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(queries ...string) rpc.HTTPError {
		url := genURL(server.URL, "/v1/files", queries...)
		req, _ := http.NewRequest(http.MethodDelete, url, nil)
		req.Header.Add(HeaderUserID, testUserID)
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		return resp2Error(resp)
	}

	{
		require.Equal(t, 400, doRequest("src", "/a").StatusCode())
		require.Equal(t, 400, doRequest("path", "a/b/../../..").StatusCode())
	}
	{
		node.TestGetUser(t, func() rpc.HTTPError { return doRequest("path", "/dir/a") }, testUserID)
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		require.Equal(t, e1.Status, doRequest("path", "/dir/a").StatusCode())
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e2)
		require.Equal(t, e2.Status, doRequest("path", "/dir/a").StatusCode())
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		node.OnceLookup(false)
		node.Volume.EXPECT().Delete(A, A, A, A).Return(e3)
		require.Equal(t, e3.Status, doRequest("path", "/dir/a").StatusCode())
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		node.OnceLookup(false)
		node.Volume.EXPECT().Delete(A, A, A, A).Return(nil)
		require.NoError(t, doRequest("path", "/dir/a"))
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		node.OnceLookup(true)
		node.Volume.EXPECT().Delete(A, A, A, A).Return(nil)
		require.NoError(t, doRequest("path", "/dir/a/"))
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		node.Volume.EXPECT().Delete(A, A, A, A).Return(nil)
		require.NoError(t, doRequest("path", "a"))
	}

	// recursively delete
	doRecuDel := func() rpc.HTTPError { return doRequest("recursive", "1", "path", "/dir/a/") }
	{
		node.TestGetUser(t, doRecuDel, testUserID)
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		require.Equal(t, e1.Status, doRecuDel().StatusCode())
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)  // for /dir
		node.OnceLookup(false) // for ./a
		require.Equal(t, sdk.ErrNotDir.Status, doRecuDel().StatusCode())
	}
	{
		node.OnceGetUser()
		node.LookupDirN(2)
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e2)
		require.Equal(t, e2.Status, doRecuDel().StatusCode())
	}
	{
		node.OnceGetUser()
		node.LookupDirN(3)
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e3)
		require.Equal(t, e3.Status, doRecuDel().StatusCode())
	}
	{
		node.OnceGetUser()
		node.LookupDirN(3)
		node.OnceLookup(false)
		require.Equal(t, sdk.ErrNotEmpty.Status, doRecuDel().StatusCode())
	}
	{
		node.OnceGetUser()
		node.LookupDirN(4)
		node.Volume.EXPECT().Readdir(A, A, A, A).Return(nil, e4)
		require.Equal(t, e4.Status, doRecuDel().StatusCode())
	}
	{
		node.OnceGetUser()
		node.LookupDirN(4)
		node.ListDir(10000, 0)
		require.Equal(t, sdk.ErrInternalServerError.Status, doRecuDel().StatusCode())
	}
	{
		node.OnceGetUser()
		node.LookupDirN(4)
		node.ListDir(100, 1)
		require.Equal(t, sdk.ErrNotDir.Status, doRecuDel().StatusCode())
	}
	{
		node.OnceGetUser()
		node.LookupDirN(4)
		node.ListDir(10, 0)
		for range [10]struct{}{} {
			node.OnceLookup(true)
			node.ListDir(10, 0)
		}
		for range [100]struct{}{} {
			node.OnceLookup(true)
			node.ListDir(0, 0)
		}
		node.Volume.EXPECT().Delete(A, A, A, A).Return(nil).Times(111)
		require.NoError(t, doRecuDel())
	}
	{
		node.OnceGetUser()
		node.LookupDirN(4)
		node.ListDir(100, 0)
		for range [100]struct{}{} {
			node.OnceLookup(true)
			node.ListDir(0, 0)
		}
		node.Volume.EXPECT().Delete(A, A, A, A).Return(e1).MinTimes(1)
		require.Equal(t, e1.Status, doRecuDel().StatusCode())
	}
}

func TestHandleBatchDelete(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(args ArgsBatchDelete) (*BatchDeleteResult, rpc.HTTPError) {
		url := genURL(server.URL, "/v1/files/batch")
		data, _ := json.Marshal(args)
		req, _ := http.NewRequest(http.MethodDelete, url, bytes.NewReader(data))
		req.Header.Add(HeaderUserID, testUserID)
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()

		res := &BatchDeleteResult{}
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		json.Unmarshal(body, res)

		return res, resp2Error(resp)
	}

	{
		node.OnceGetUser(testUserID)
		node.Volume.EXPECT().Lookup(A, A, A).Return(&sdk.DirInfo{}, nil).Times(2)
		node.Volume.EXPECT().Delete(A, A, A, A).Return(nil)
		args := ArgsBatchDelete{
			Paths: []string{"/test/1234"},
		}

		res, err := doRequest(args)
		require.NoError(t, err)
		require.True(t, reflect.DeepEqual(res.Deleted, args.Paths))
	}
}
