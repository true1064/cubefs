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
		req.Header.Add(headerRequestID, "user_request_id")
		req.Header.Add(headerUserID, testUserID)
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		err = rpc.ParseData(resp, nil)
		if err != nil {
			return err.(rpc.HTTPError)
		}
		return nil
	}

	{
		require.Equal(t, 400, doRequest("src", "/a").StatusCode())
		require.Equal(t, 400, doRequest("path", "a/b/../../..").StatusCode())
	}
	{
		node.OnceGetUser(testUserID)
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, &sdk.Error{Status: 521})
		require.Equal(t, 521, doRequest("path", "/dira/dirb/").StatusCode())
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
		req.Header.Add(headerUserID, testUserID)
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		err = rpc.ParseData(resp, nil)
		if err != nil {
			return err.(rpc.HTTPError)
		}
		return nil
	}

	{
		require.Equal(t, 400, doRequest("src", "/a").StatusCode())
		require.Equal(t, 400, doRequest("path", "a/b/../../..").StatusCode())
	}
	{
		node.OnceGetUser(testUserID)
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, &sdk.Error{Status: 521})
		require.Equal(t, 521, doRequest("path", "/dir/a").StatusCode())
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, &sdk.Error{Status: 522})
		require.Equal(t, 522, doRequest("path", "/dir/a").StatusCode())
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		node.OnceLookup(false)
		node.Volume.EXPECT().Delete(A, A, A, A).Return(&sdk.Error{Status: 523})
		require.Equal(t, 523, doRequest("path", "/dir/a").StatusCode())
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
}
