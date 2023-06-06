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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/apinode/sdk"
)

func TestHandleFileUpload(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(body *mockBody, queries ...string) *http.Response {
		url := genURL(server.URL, "/v1/files/upload", queries...)
		req, _ := http.NewRequest(http.MethodPut, url, body)
		req.Header.Add(headerUserID, testUserID)
		req.Header.Add(headerCrc32, fmt.Sprint(body.Sum32()))
		req.Header.Add(userPropertyPrefix+"upload", "Uploaded-")
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		return resp
	}

	{
		// no path
		resp := doRequest(newMockBody(64))
		defer resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)
	}
	{
		// invalid file path
		resp := doRequest(newMockBody(64), "path", "../../filename")
		defer resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)
	}
	{
		node.OnceGetUser(testUserID)
		// create dir error
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		resp := doRequest(newMockBody(64), "path", "/dir/a/../filename")
		defer resp.Body.Close()
		require.Equal(t, e1.Status, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		node.OnceGetInode()
		// invalid crc
		url := genURL(server.URL, "/v1/files/upload", "path", "/dir/a/../filename")
		req, _ := http.NewRequest(http.MethodPost, url, newMockBody(64))
		req.Header.Add(headerUserID, testUserID)
		req.Header.Add(headerCrc32, "invalid-crc-32")
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		node.OnceGetInode()
		node.Volume.EXPECT().UploadFile(A, A).Return(nil, e2)
		// uploda file error
		resp := doRequest(newMockBody(64), "path", "/dir/a/../filename")
		defer resp.Body.Close()
		require.Equal(t, e2.Status, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		node.OnceGetInode()
		node.Volume.EXPECT().UploadFile(A, A).Return(&sdk.InodeInfo{Inode: node.GenInode()}, nil)
		// uploda file error
		resp := doRequest(newMockBody(64), "path", "/dir/a/../filename")
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)

		buff, _ := io.ReadAll(resp.Body)
		var file FileInfo
		require.NoError(t, json.Unmarshal(buff, &file))
		require.Equal(t, "filename", file.Name)
		require.Equal(t, "Uploaded-", file.Properties["upload"])
	}
}

func TestHandleFileWrite(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	file := newMockBody(1024)
	node.Volume.EXPECT().ReadFile(A, A, A, A).DoAndReturn(
		func(_ context.Context, _, _ uint64, p []byte) (int, error) {
			return file.Read(p)
		}).AnyTimes()

	doRequest := func(body *mockBody, ranged string, queries ...string) *http.Response {
		url := genURL(server.URL, "/v1/files/content", queries...)
		req, _ := http.NewRequest(http.MethodPut, url, body)
		req.Header.Add(headerUserID, testUserID)
		req.Header.Add(headerCrc32, fmt.Sprint(body.Sum32()))
		if ranged != "" {
			req.Header.Add(headerRange, ranged)
		}
		if cl := len(body.buff); cl > 1 {
			req.ContentLength = int64(cl)
		}
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		return resp
	}

	{
		resp := doRequest(newMockBody(64), "")
		defer resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)
	}
	{
		resp := doRequest(newMockBody(64), "", "path", "../a", "fileId", "1111")
		defer resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)
	}
	queries := []string{"path", "/a", "fileId", "1111"}
	{
		node.OnceGetUser(testUserID)
		node.Volume.EXPECT().GetInode(A, A).Return(nil, e1)
		resp := doRequest(newMockBody(64), "", queries...)
		defer resp.Body.Close()
		require.Equal(t, e1.Status, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().GetInode(A, A).Return(&sdk.InodeInfo{Size: 1024}, nil)
		resp := doRequest(newMockBody(64), "bytes=i-j", queries...)
		defer resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().GetInode(A, A).Return(&sdk.InodeInfo{Size: 1024}, nil)
		resp := doRequest(newMockBody(1), "bytes=1-", queries...)
		defer resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().GetInode(A, A).Return(&sdk.InodeInfo{Size: 1024}, nil)
		resp := doRequest(newMockBody(64), "bytes=1025-", queries...)
		defer resp.Body.Close()
		require.Equal(t, sdk.ErrWriteOverSize.Status, resp.StatusCode)
	}
	{
		file = newMockBody(1024)
		node.OnceGetUser()
		node.Volume.EXPECT().GetInode(A, A).Return(&sdk.InodeInfo{Size: 1024}, nil)
		node.Volume.EXPECT().WriteFile(A, A, A, A, A).Return(e2)
		resp := doRequest(newMockBody(64), "bytes=100-", queries...)
		defer resp.Body.Close()
		require.Equal(t, e2.Status, resp.StatusCode)
	}
	{
		file = newMockBody(1024)
		node.OnceGetUser()
		node.Volume.EXPECT().GetInode(A, A).Return(&sdk.InodeInfo{Size: 1024}, nil)
		node.Volume.EXPECT().WriteFile(A, A, A, A, A).Return(nil)
		resp := doRequest(newMockBody(64), "bytes=100-", queries...)
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().GetInode(A, A).Return(&sdk.InodeInfo{Size: 1024}, nil)
		body := newMockBody(1024)
		origin := body.buff[:]
		file = &mockBody{buff: origin}
		node.Volume.EXPECT().WriteFile(A, A, A, A, A).DoAndReturn(
			func(_ context.Context, _, _, size uint64, r io.Reader) error {
				buff := make([]byte, size)
				io.ReadFull(r, buff)
				expect := origin[:100]
				expect = append(expect, origin[:]...)
				require.Equal(t, expect, buff)
				return nil
			})
		resp := doRequest(body, "bytes=100-", queries...)
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)
	}
}

func TestHandleFileDownload(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(body *mockBody, ranged string, queries ...string) *http.Response {
		url := genURL(server.URL, "/v1/files/content", queries...)
		req, _ := http.NewRequest(http.MethodGet, url, body)
		req.Header.Add(headerUserID, testUserID)
		if ranged != "" {
			req.Header.Add(headerRange, ranged)
		}
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		return resp
	}

	{
		resp := doRequest(newMockBody(64), "")
		defer resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)
	}
	{
		resp := doRequest(newMockBody(64), "", "path", "../")
		defer resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)
	}
	{
		node.OnceGetUser(testUserID)
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		resp := doRequest(newMockBody(64), "", "path", "/download")
		defer resp.Body.Close()
		require.Equal(t, e1.Status, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.OnceLookup(false)
		node.Volume.EXPECT().GetInode(A, A).Return(nil, e2)
		resp := doRequest(newMockBody(64), "", "path", "/download")
		defer resp.Body.Close()
		require.Equal(t, e2.Status, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.OnceLookup(false)
		node.OnceGetInode()
		resp := doRequest(newMockBody(64), "bytes=i-j", "path", "/download")
		defer resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)
	}
	{
		size := 128
		node.OnceGetUser()
		node.OnceLookup(false)
		node.OnceGetInode()
		body := newMockBody(size)
		node.Volume.EXPECT().ReadFile(A, A, A, A).DoAndReturn(
			func(_ context.Context, _, _ uint64, p []byte) (int, error) {
				copy(p, body.buff[:size])
				return size, io.EOF
			})
		resp := doRequest(nil, "", "path", "/download")
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)
		buff, _ := io.ReadAll(resp.Body)
		require.Equal(t, body.buff[:size], buff)
	}
	{
		size := 1024
		node.OnceGetUser()
		node.OnceLookup(false)
		node.OnceGetInode()
		body := newMockBody(size)
		origin := body.buff[size-28 : size]
		node.Volume.EXPECT().ReadFile(A, A, A, A).DoAndReturn(
			func(_ context.Context, _, _ uint64, p []byte) (int, error) {
				return body.Read(p)
			}).Times(2)
		resp := doRequest(nil, "bytes=-28", "path", "/download")
		defer resp.Body.Close()
		require.Equal(t, 206, resp.StatusCode)
		buff, _ := io.ReadAll(resp.Body)
		require.Equal(t, origin, buff)
	}
}

func TestHandleFileRename(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(queries ...string) rpc.HTTPError {
		url := genURL(server.URL, "/v1/files/rename", queries...)
		req, _ := http.NewRequest(http.MethodPost, url, nil)
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
		require.Equal(t, 400, doRequest("src", "/a", "dst", "a/b/../../..").StatusCode())
	}
	{
		node.OnceGetUser(testUserID)
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		require.Equal(t, e1.Status, doRequest("src", "/dir/a", "dst", "/dir/b").StatusCode())
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e2)
		require.Equal(t, e2.Status, doRequest("src", "/dir/a", "dst", "/dir/b").StatusCode())
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		node.OnceLookup(true)
		node.Volume.EXPECT().Rename(A, A, A, A, A).Return(e3)
		require.Equal(t, e3.Status, doRequest("src", "/dir/a/", "dst", "/dir/b/").StatusCode())
	}
	for _, cs := range []struct {
		lookup   int
		src, dst string
	}{
		{lookup: 2, src: "/dir/a", dst: "/dir/b"},
		{lookup: 2, src: "dir/a", dst: "dir/b"},
		{lookup: 1, src: "/dir/a", dst: "/b"},
		{lookup: 1, src: "/a", dst: "/dir/b"},
		{lookup: 0, src: "a", dst: "/b"},
		{lookup: 0, src: "a", dst: "b"},
	} {
		node.OnceGetUser()
		for i := 0; i < cs.lookup; i++ {
			node.OnceLookup(true)
		}
		node.Volume.EXPECT().Rename(A, A, A, A, A).Return(nil)
		require.NoError(t, doRequest("src", cs.src, "dst", cs.dst))
	}
}

func TestHandleFileCopy(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(queries ...string) rpc.HTTPError {
		url := genURL(server.URL, "/v1/files/copy", queries...)
		req, _ := http.NewRequest(http.MethodPost, url, nil)
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
		require.Equal(t, 400, doRequest("src", "/a", "dst", "a/b/../../..").StatusCode())
	}
	{
		node.OnceGetUser(testUserID)
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		require.Equal(t, e1.Status, doRequest("src", "/dir/a", "dst", "/dir/b").StatusCode())
	}
	{
		node.OnceGetUser()
		node.LookupN(2)
		node.Volume.EXPECT().GetInode(A, A).Return(nil, e2)
		require.Equal(t, e2.Status, doRequest("src", "/dir/a", "dst", "/dir/b").StatusCode())
	}
	{
		node.OnceGetUser()
		node.LookupN(2)
		node.OnceGetInode()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e3)
		require.Equal(t, e3.Status, doRequest("src", "/dir/a", "dst", "/dir/b").StatusCode())
	}
	{
		node.OnceGetUser()
		node.LookupN(2)
		node.OnceGetInode()
		node.OnceLookup(true)
		node.Volume.EXPECT().GetInode(A, A).Return(nil, e4)
		require.Equal(t, e4.Status, doRequest("src", "/dir/a", "dst", "/dir/b").StatusCode())
	}
	{
		node.OnceGetUser()
		node.LookupN(2)
		node.OnceGetInode()
		node.OnceLookup(true)
		node.OnceGetInode()
		node.Volume.EXPECT().UploadFile(A, A).Return(nil, nil)
		require.NoError(t, doRequest("src", "/dir/a", "dst", "/dir/b"))
	}
	{
		node.OnceGetUser()
		node.LookupN(2)
		node.OnceGetInode()
		node.OnceLookup(true)
		node.OnceGetInode()
		node.Volume.EXPECT().GetXAttrMap(A, A).Return(nil, nil)
		node.Volume.EXPECT().UploadFile(A, A).Return(nil, nil)
		require.NoError(t, doRequest("src", "/dir/a", "dst", "/dir/b", "meta", "1"))
	}
}
