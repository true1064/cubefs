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
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

var uploadID = "upload-id-foo-bar"

func TestHandleMultipartUploads(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(body *mockBody, r interface{}, queries ...string) rpc.HTTPError {
		url := genURL(server.URL, "/v1/files/multipart", queries...)
		req, _ := http.NewRequest(http.MethodPost, url, body)
		req.ContentLength = int64(len(body.buff))
		req.Header.Add(headerUserID, testUserID)
		req.Header.Add(headerCrc32, fmt.Sprint(body.Sum32()))
		req.Header.Add(userPropertyPrefix+"multipart", "MultiPart")
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		if err = rpc.ParseData(resp, r); err != nil {
			return err.(rpc.HTTPError)
		}
		return nil
	}

	{
		require.Equal(t, 400, doRequest(newMockBody(0), nil, "pathx", "/a").StatusCode())
		require.Equal(t, 400, doRequest(newMockBody(0), nil, "path", "a/../../b").StatusCode())
	}
	{
		node.OnceGetUser(testUserID)
		node.Volume.EXPECT().InitMultiPart(A, A, A, A).Return("", &sdk.Error{Status: 521})
		var up RespMPuploads
		require.Equal(t, 521, doRequest(newMockBody(0), &up, "path", "/mpfile").StatusCode())
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().InitMultiPart(A, A, A, A).Return(uploadID, nil)
		var up RespMPuploads
		require.NoError(t, doRequest(newMockBody(0), &up, "path", "/mpfile"))
		require.Equal(t, uploadID, up.UploadID)
	}
	{
		node.OnceGetUser()
		require.Equal(t, 400, doRequest(newMockBody(64), nil, "path", "/mpfile", "uploadId", uploadID).StatusCode())
	}
	{
		node.OnceGetUser()
		var parts []MPPart
		for range [10]struct{}{} {
			parts = append(parts, MPPart{})
		}
		buff, _ := json.Marshal(parts)
		body := &mockBody{buff: buff}
		node.Volume.EXPECT().CompleteMultiPart(A, A, A, A, A).Return(nil, &sdk.Error{Status: 522})
		require.Equal(t, 522, doRequest(body, nil, "path", "/mpfile", "uploadId", uploadID).StatusCode())
	}
	{
		node.OnceGetUser()
		body := &mockBody{buff: []byte("[]")}
		node.Volume.EXPECT().CompleteMultiPart(A, A, A, A, A).Return(&sdk.InodeInfo{Inode: node.GenInode()}, nil)
		node.Volume.EXPECT().GetXAttrMap(A, A).Return(nil, &sdk.Error{Status: 523})
		require.Equal(t, 523, doRequest(body, nil, "path", "/mpfile", "uploadId", uploadID).StatusCode())
	}
	{
		node.OnceGetUser()
		body := &mockBody{buff: []byte("[]")}
		node.Volume.EXPECT().CompleteMultiPart(A, A, A, A, A).Return(&sdk.InodeInfo{Inode: node.GenInode()}, nil)
		node.Volume.EXPECT().GetXAttrMap(A, A).Return(nil, nil)
		require.NoError(t, doRequest(body, nil, "path", "mpfile", "uploadId", uploadID))
	}
}

func TestHandleMultipartParts(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(body *mockBody, r interface{}, queries ...string) rpc.HTTPError {
		url := genURL(server.URL, "/v1/files/multipart", queries...)
		req, _ := http.NewRequest(http.MethodPut, url, body)
		req.ContentLength = int64(len(body.buff))
		req.Header.Add(headerUserID, testUserID)
		req.Header.Add(headerCrc32, fmt.Sprint(body.Sum32()))
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		if err = rpc.ParseData(resp, r); err != nil {
			return err.(rpc.HTTPError)
		}
		return nil
	}

	{
		require.Equal(t, 400, doRequest(newMockBody(0), nil, "path", "/a").StatusCode())
		require.Equal(t, 400, doRequest(newMockBody(0), nil, "path", "/a", "uploadId", uploadID).StatusCode())
		require.Equal(t, 400, doRequest(newMockBody(0), nil, "path", "/a", "uploadId", uploadID, "partNumber", "x").StatusCode())
		require.Equal(t, 400, doRequest(newMockBody(0), nil, "path", "/a", "uploadId", uploadID, "partNumber", "0").StatusCode())
	}
	{
		node.OnceGetUser(testUserID)
		node.Volume.EXPECT().UploadMultiPart(A, A, A, A, A).Return(nil, &sdk.Error{Status: 521})
		require.Equal(t, 521, doRequest(newMockBody(0), nil, "path", "/a", "uploadId", uploadID, "partNumber", "1").StatusCode())
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().UploadMultiPart(A, A, A, A, A).Return(&sdk.Part{MD5: "md5"}, nil)
		var part MPPart
		require.NoError(t, doRequest(newMockBody(0), &part, "path", "/a", "uploadId", uploadID, "partNumber", "1"))
		require.Equal(t, "md5", part.MD5)
	}
}

func TestHandleMultipartList(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(r interface{}, queries ...string) rpc.HTTPError {
		url := genURL(server.URL, "/v1/files/multipart", queries...)
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		req.Header.Add(headerUserID, testUserID)
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		if err = rpc.ParseData(resp, r); err != nil {
			return err.(rpc.HTTPError)
		}
		return nil
	}

	{
		require.Equal(t, 400, doRequest(nil, "path", "/a").StatusCode())
		require.Equal(t, 400, doRequest(nil, "path", "/a", "uploadId", uploadID).StatusCode())
		require.Equal(t, 400, doRequest(nil, "path", "/a", "uploadId", uploadID, "marker", "x").StatusCode())
		require.Equal(t, 400, doRequest(nil, "path", "a/../..", "uploadId", uploadID, "marker", "10").StatusCode())
	}
	{
		node.OnceGetUser(testUserID)
		node.Volume.EXPECT().ListMultiPart(A, A, A, A, A).Return(nil, uint64(0), false, &sdk.Error{Status: 521})
		require.Equal(t, 521, doRequest(nil, "path", "/a", "uploadId", uploadID, "marker", "10").StatusCode())
	}
	{
		node.OnceGetUser()
		var parts []*sdk.Part
		for i := range [10]struct{}{} {
			parts = append(parts, &sdk.Part{ID: uint16(i) + 1})
		}
		node.Volume.EXPECT().ListMultiPart(A, A, A, A, A).Return(parts, uint64(100), true, nil)
		var part RespMPList
		require.NoError(t, doRequest(&part, "path", "/a", "uploadId", uploadID, "marker", "0"))
		require.Equal(t, 10, len(part.Parts))
		require.Equal(t, FileID(100), part.Next)
	}
}

func TestHandleMultipartAbort(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(queries ...string) rpc.HTTPError {
		url := genURL(server.URL, "/v1/files/multipart", queries...)
		req, _ := http.NewRequest(http.MethodDelete, url, nil)
		req.Header.Add(headerUserID, testUserID)
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		if err = rpc.ParseData(resp, nil); err != nil {
			return err.(rpc.HTTPError)
		}
		return nil
	}

	{
		require.Equal(t, 400, doRequest("path", "/a").StatusCode())
		require.Equal(t, 400, doRequest("path", "/a", "uploadIdx", uploadID).StatusCode())
	}
	{
		node.OnceGetUser(testUserID)
		node.Volume.EXPECT().AbortMultiPart(A, A, A).Return(&sdk.Error{Status: 521})
		require.Equal(t, 521, doRequest("path", "/a", "uploadId", uploadID).StatusCode())
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().AbortMultiPart(A, A, A).Return(nil)
		require.NoError(t, doRequest("path", "/a", "uploadId", uploadID))
	}
}
