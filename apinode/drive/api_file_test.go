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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/apinode/sdk"
)

func TestHandleFileUpload(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(body *mockBody, querys ...string) *http.Response {
		url := genURL(server.URL, "/v1/files/upload", querys...)
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
		node.OnceGetUser()
		// create dir error
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, &sdk.Error{Status: 521})
		resp := doRequest(newMockBody(64), "path", "/dir/a/../filename")
		defer resp.Body.Close()
		require.Equal(t, 521, resp.StatusCode)
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
		node.Volume.EXPECT().UploadFile(A).Return(nil, &sdk.Error{Status: 522})
		// uploda file error
		resp := doRequest(newMockBody(64), "path", "/dir/a/../filename")
		defer resp.Body.Close()
		require.Equal(t, 522, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		node.OnceGetInode()
		node.Volume.EXPECT().UploadFile(A).DoAndReturn(
			func(_ *sdk.UploadFileReq) (*sdk.InodeInfo, error) {
				return &sdk.InodeInfo{
					Inode: node.GenInode(),
				}, nil
			})
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

	doRequest := func(body *mockBody, ranged string, querys ...string) *http.Response {
		url := genURL(server.URL, "/v1/files/content", querys...)
		req, _ := http.NewRequest(http.MethodPut, url, body)
		req.Header.Add(headerUserID, testUserID)
		req.Header.Add(headerCrc32, fmt.Sprint(body.Sum32()))
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
		node.OnceGetUser()
		node.Volume.EXPECT().GetInode(A, A).Return(nil, &sdk.Error{Status: 521})
		resp := doRequest(newMockBody(64), "", "fileId", "1111")
		defer resp.Body.Close()
		require.Equal(t, 521, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().GetInode(A, A).Return(&sdk.InodeInfo{Size: 1024}, nil)
		resp := doRequest(newMockBody(64), "bytes=i-j", "fileId", "1111")
		defer resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().GetInode(A, A).Return(&sdk.InodeInfo{Size: 1024}, nil)
		node.Volume.EXPECT().WriteFile(A, A, A, A, A).Return(&sdk.Error{Status: 522})
		resp := doRequest(newMockBody(64), "bytes=100-", "fileId", "1111")
		defer resp.Body.Close()
		require.Equal(t, 522, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().GetInode(A, A).Return(&sdk.InodeInfo{Size: 1024}, nil)
		node.Volume.EXPECT().WriteFile(A, A, A, A, A).Return(nil)
		resp := doRequest(newMockBody(64), "bytes=100-", "fileId", "1111")
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)
	}
}
