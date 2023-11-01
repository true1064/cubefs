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
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/apinode/crypto"
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/proto"
)

var uploadID = "upload-id-foo-bar"

func TestHandleMultipartUploads(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	contentMD5 := ""
	doRequest := func(body *mockBody, r interface{}, queries ...string) rpc.HTTPError {
		url := genURL(server.URL, "/v1/files/multipart", queries...)
		req, _ := http.NewRequest(http.MethodPost, url, body)
		req.ContentLength = int64(len(body.buff))
		req.Header.Add(HeaderUserID, testUserID.ID)
		req.Header.Add(HeaderCrc32, fmt.Sprint(body.Sum32()))
		req.Header.Add(EncodeMetaHeader("multipart"), EncodeMeta("MultiPart"))
		if contentMD5 != "" {
			req.Header.Add(HeaderMD5, contentMD5)
		}
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		return resp2Data(resp, r)
	}

	{
		require.Equal(t, 400, doRequest(newMockBody(0), nil, "pathx", "/a").StatusCode())
		require.Equal(t, 400, doRequest(newMockBody(0), nil, "path", "a/../../b").StatusCode())
	}
	{
		node.TestGetUser(t, func() rpc.HTTPError {
			return doRequest(newMockBody(0), nil, "path", "/mpfile")
		}, testUserID)
		node.TestGetUser(t, func() rpc.HTTPError {
			return doRequest(newMockBody(0), nil, "path", "/mpfile", "uploadId", uploadID)
		}, testUserID)
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		node.Volume.EXPECT().InitMultiPart(A, A, A).Return("", e1)
		var up RespMPuploads
		require.Equal(t, e1.Status, doRequest(newMockBody(0), &up, "path", "/mpfile").StatusCode())
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		node.Volume.EXPECT().InitMultiPart(A, A, A).Return(uploadID, nil)
		var up RespMPuploads
		require.NoError(t, doRequest(newMockBody(0), &up, "path", "/mpfile"))
		require.Equal(t, uploadID, up.UploadID)
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		require.Equal(t, 400, doRequest(newMockBody(64), nil, "path", "/mpfile", "uploadId", uploadID).StatusCode())
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		require.Equal(t, sdk.ErrConflict.Status, doRequest(newMockBody(0), nil,
			"path", "/mpfile", "uploadId", uploadID, "fileId", "123").StatusCode())
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(&sdk.DirInfo{Inode: 11111}, nil)
		node.Volume.EXPECT().GetXAttrMap(A, A).Return(nil, e1)
		node.Volume.EXPECT().GetInode(A, A).Return(nil, nil)
		require.Equal(t, e1.Status, doRequest(newMockBody(0), nil,
			"path", "/mpfile", "uploadId", uploadID, "fileId", "123").StatusCode())
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(&sdk.DirInfo{Inode: 11111}, nil)
		node.Volume.EXPECT().GetXAttrMap(A, A).Return(make(map[string]string), nil)
		node.Volume.EXPECT().GetInode(A, A).Return(nil, nil)
		require.Equal(t, sdk.ErrConflict.Status, doRequest(newMockBody(0), nil,
			"path", "/mpfile", "uploadId", uploadID, "fileId", "123").StatusCode())
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(&sdk.DirInfo{Inode: 11111}, nil)
		node.Volume.EXPECT().GetXAttrMap(A, A).Return(map[string]string{internalMetaUploadID: uploadID}, nil)
		node.Volume.EXPECT().GetInode(A, A).Return(&proto.InodeInfo{Size: 1024}, nil)
		require.NoError(t, doRequest(newMockBody(0), nil, "path", "/mpfile", "uploadId", uploadID, "fileId", "123"))
	}
	{
		node.OnceGetUser()
		body := &mockBody{buff: []byte("[]")}
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrConflict)
		require.Equal(t, sdk.ErrConflict.Status, doRequest(body, nil, "path", "/mpfile", "uploadId", uploadID, "fileId", "123").StatusCode())
	}
	{
		node.OnceGetUser()
		body := &mockBody{buff: []byte("[]")}
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		node.Volume.EXPECT().ListMultiPart(A, A, A, A, A).Return(nil, uint64(0), false, e2)
		require.Equal(t, e2.Status, doRequest(body, nil, "path", "/mpfile", "uploadId", uploadID).StatusCode())
	}
	{
		node.OnceGetUser()
		var listp []*sdk.Part
		for idx := range [10]struct{}{} {
			listp = append(listp, &sdk.Part{ID: uint16(idx + 1), Size: crypto.BlockSize, MD5: fmt.Sprint(idx)})
		}
		body := &mockBody{buff: []byte("[]")}
		listp[0].Size = crypto.BlockSize - 1
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		node.Volume.EXPECT().ListMultiPart(A, A, A, A, A).Return(listp, uint64(0), false, nil)
		node.Volume.EXPECT().AbortMultiPart(A, A, A).Return(nil)
		require.Equal(t, 400, doRequest(body, nil, "path", "/mpfile", "uploadId", uploadID).StatusCode())
	}
	{
		node.OnceGetUser()
		var listp []*sdk.Part
		for idx := range [10]struct{}{} {
			listp = append(listp, &sdk.Part{ID: uint16(idx + 1), Size: crypto.BlockSize, MD5: fmt.Sprint(idx)})
		}
		body := &mockBody{buff: []byte("[]")}
		listp[9].Size = crypto.BlockSize - 1
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		node.Volume.EXPECT().ListMultiPart(A, A, A, A, A).Return(listp, uint64(1), true, nil)
		node.Volume.EXPECT().AbortMultiPart(A, A, A).Return(e4)
		require.Equal(t, 400, doRequest(body, nil, "path", "/mpfile", "uploadId", uploadID).StatusCode())
	}
	{
		node.OnceGetUser()
		var parts []MPPart
		var listp []*sdk.Part
		for idx := range [10]struct{}{} {
			parts = append(parts, MPPart{PartNumber: uint16(idx + 1), Size: int(crypto.BlockSize), Etag: fmt.Sprint(idx)})
			listp = append(listp, &sdk.Part{ID: uint16(idx + 1), Size: crypto.BlockSize, MD5: fmt.Sprint(idx)})
		}
		buff, _ := json.Marshal(parts)
		body := &mockBody{buff: buff}
		listp[5].MD5 = "xxx"
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		node.Volume.EXPECT().ListMultiPart(A, A, A, A, A).Return(listp, uint64(0), false, nil)
		require.Equal(t, 400, doRequest(body, nil, "path", "/mpfile", "uploadId", uploadID).StatusCode())
	}
	{
		node.OnceGetUser()
		var parts []MPPart
		var listp []*sdk.Part
		for idx := range [10]struct{}{} {
			parts = append(parts, MPPart{PartNumber: uint16(idx + 1), Size: int(crypto.BlockSize), Etag: fmt.Sprint(idx)})
			listp = append(listp, &sdk.Part{ID: uint16(idx + 1), Size: crypto.BlockSize, MD5: fmt.Sprint(idx)})
		}
		buff, _ := json.Marshal(parts)
		body := &mockBody{buff: buff}
		listp = listp[:8]
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		node.Volume.EXPECT().ListMultiPart(A, A, A, A, A).Return(listp, uint64(0), false, nil)
		require.Equal(t, 400, doRequest(body, nil, "path", "/mpfile", "uploadId", uploadID).StatusCode())
	}
	{
		node.OnceGetUser()
		body := &mockBody{buff: []byte("[]")}
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		node.Volume.EXPECT().ListMultiPart(A, A, A, A, A).Return(nil, uint64(100), false, nil)
		node.Volume.EXPECT().ListMultiPart(A, A, A, A, A).Return(nil, uint64(0), false, nil)
		node.Volume.EXPECT().CompleteMultiPart(A, A).Return(&sdk.InodeInfo{Inode: node.GenInode()}, uint64(0), nil)
		node.Volume.EXPECT().GetXAttrMap(A, A).Return(nil, e3)
		require.Equal(t, e3.Status, doRequest(body, nil, "path", "/mpfile", "uploadId", uploadID).StatusCode())
	}
	{
		node.OnceGetUser()
		body := &mockBody{buff: []byte("[]")}
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		node.Volume.EXPECT().ListMultiPart(A, A, A, A, A).Return(nil, uint64(100), false, nil)
		node.Volume.EXPECT().ListMultiPart(A, A, A, A, A).Return(nil, uint64(0), false, nil)
		node.Volume.EXPECT().CompleteMultiPart(A, A).Return(nil, uint64(0), e4)
		require.Equal(t, e4.Status, doRequest(body, nil, "path", "/mpfile", "uploadId", uploadID).StatusCode())
	}
	{
		node.OnceGetUser()
		var listp []*sdk.Part
		for idx := range [10]struct{}{} {
			listp = append(listp, &sdk.Part{ID: uint16(idx + 1), Size: crypto.BlockSize, MD5: fmt.Sprint(idx)})
		}
		body := &mockBody{buff: []byte("[]")}
		listp[9].Size = crypto.BlockSize - 1
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		node.Volume.EXPECT().ListMultiPart(A, A, A, A, A).Return(nil, uint64(0), false, nil)
		node.Volume.EXPECT().CompleteMultiPart(A, A).Return(&sdk.InodeInfo{Inode: node.GenInode()}, uint64(0), nil)
		node.Volume.EXPECT().GetXAttrMap(A, A).Return(nil, nil)
		require.NoError(t, doRequest(body, nil, "path", "/mpfile", "uploadId", uploadID))
	}
	{
		node.OnceGetUser()
		var parts []MPPart
		var listp []*sdk.Part
		for idx := range [1]struct{}{} {
			parts = append(parts, MPPart{PartNumber: uint16(idx + 1), Size: int(crypto.BlockSize), Etag: fmt.Sprint(idx)})
			listp = append(listp, &sdk.Part{ID: uint16(idx + 1), Inode: uint64(idx + 1111), Size: crypto.BlockSize, MD5: fmt.Sprint(idx)})
		}
		buff, _ := json.Marshal(parts)
		body := &mockBody{buff: buff}
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		node.Volume.EXPECT().ListMultiPart(A, A, A, A, A).Return(listp, uint64(0), false, nil)
		node.Volume.EXPECT().ReadFile(A, A, A, A).Return(int(crypto.BlockSize-1), nil)
		require.Equal(t, 500, doRequest(body, nil, "path", "/mpfile", "uploadId", uploadID).StatusCode())
	}
	{
		node.OnceGetUser()
		var parts []MPPart
		var listp []*sdk.Part
		for idx := range [10]struct{}{} {
			parts = append(parts, MPPart{PartNumber: uint16(idx + 1), Size: int(crypto.BlockSize), Etag: fmt.Sprint(idx)})
			listp = append(listp, &sdk.Part{ID: uint16(idx + 1), Inode: uint64(idx + 1111), Size: crypto.BlockSize, MD5: fmt.Sprint(idx)})
		}
		buff, _ := json.Marshal(parts)
		body := &mockBody{buff: buff}
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		node.Volume.EXPECT().ListMultiPart(A, A, A, A, A).Return(listp, uint64(0), false, nil)
		node.Volume.EXPECT().ReadFile(A, A, A, A).Return(int(crypto.BlockSize), nil).Times(10)
		node.Volume.EXPECT().CompleteMultiPart(A, A).DoAndReturn(
			func(_ context.Context, req *sdk.CompleteMultipartReq) (*sdk.InodeInfo, uint64, error) {
				require.NotEmpty(t, req.Extend[internalMetaMD5])
				require.NotEmpty(t, req.Extend[internalMetaUploadID])
				return &sdk.InodeInfo{Inode: node.GenInode()}, uint64(0), nil
			})
		node.Volume.EXPECT().GetXAttrMap(A, A).Return(nil, nil)
		require.NoError(t, doRequest(body, nil, "path", "/mpfile", "uploadId", uploadID))
	}
	{
		node.OnceGetUser()
		var parts []MPPart
		var listp []*sdk.Part
		for idx := range [10]struct{}{} {
			parts = append(parts, MPPart{PartNumber: uint16(idx + 1), Size: int(crypto.BlockSize), Etag: fmt.Sprint(idx)})
			listp = append(listp, &sdk.Part{ID: uint16(idx + 1), Inode: uint64(idx + 1111), Size: crypto.BlockSize, MD5: fmt.Sprint(idx)})
		}
		buff, _ := json.Marshal(parts)
		body := &mockBody{buff: buff}
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		node.Volume.EXPECT().ListMultiPart(A, A, A, A, A).Return(listp, uint64(0), false, nil)
		node.Volume.EXPECT().ReadFile(A, A, A, A).Return(int(crypto.BlockSize), nil).Times(10)
		contentMD5 = "not-md5"
		require.Equal(t, sdk.ErrMismatchChecksum.Status, doRequest(body, nil, "path", "/mpfile", "uploadId", uploadID).StatusCode())
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
		req.Header.Add(HeaderUserID, testUserID.ID)
		req.Header.Add(HeaderCrc32, fmt.Sprint(body.Sum32()))
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		return resp2Data(resp, r)
	}

	{
		require.Equal(t, 400, doRequest(newMockBody(0), nil, "path", "/a").StatusCode())
		require.Equal(t, 400, doRequest(newMockBody(0), nil, "path", "/a", "uploadId", uploadID).StatusCode())
		require.Equal(t, 400, doRequest(newMockBody(0), nil, "path", "/a", "uploadId", uploadID, "partNumber", "x").StatusCode())
		require.Equal(t, 400, doRequest(newMockBody(0), nil, "path", "/a", "uploadId", uploadID, "partNumber", "0").StatusCode())
		require.Equal(t, 400, doRequest(newMockBody(0), nil, "path", "/a", "uploadId", uploadID, "partNumber", "10000").StatusCode())
	}
	{
		node.TestGetUser(t, func() rpc.HTTPError {
			return doRequest(newMockBody(0), nil, "path", "/a", "uploadId", uploadID, "partNumber", "1")
		}, testUserID)
		node.OnceGetUser()
		node.Volume.EXPECT().UploadMultiPart(A, A, A, A, A).Return(nil, e1)
		require.Equal(t, e1.Status, doRequest(newMockBody(0), nil, "path", "/a", "uploadId", uploadID, "partNumber", "1").StatusCode())
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().UploadMultiPart(A, A, A, A, A).Return(&sdk.Part{MD5: "etag", Size: 64}, nil)
		var part MPPart
		require.NoError(t, doRequest(newMockBody(0), &part, "path", "/a", "uploadId", uploadID, "partNumber", "1"))
		require.Equal(t, "etag", part.Etag)
		require.Equal(t, 64, part.Size)
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
		req.Header.Add(HeaderUserID, testUserID.ID)
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		return resp2Data(resp, r)
	}

	{
		require.Equal(t, 400, doRequest(nil, "path", "/a").StatusCode())
		require.Equal(t, 400, doRequest(nil, "path", "/a", "uploadId", uploadID).StatusCode())
		require.Equal(t, 400, doRequest(nil, "path", "/a", "uploadId", uploadID, "marker", "x").StatusCode())
		require.Equal(t, 400, doRequest(nil, "path", "a/../..", "uploadId", uploadID, "marker", "10").StatusCode())
	}
	{
		node.TestGetUser(t, func() rpc.HTTPError {
			return doRequest(nil, "path", "/a", "uploadId", uploadID, "marker", "10")
		}, testUserID)
		node.OnceGetUser()
		node.Volume.EXPECT().ListMultiPart(A, A, A, A, A).Return(nil, uint64(0), false, e1)
		require.Equal(t, e1.Status, doRequest(nil, "path", "/a", "uploadId", uploadID, "marker", "10").StatusCode())
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
		req.Header.Add(HeaderUserID, testUserID.ID)
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		return resp2Error(resp)
	}

	{
		require.Equal(t, 400, doRequest("path", "/a").StatusCode())
		require.Equal(t, 400, doRequest("path", "/a", "uploadIdx", uploadID).StatusCode())
	}
	{
		node.TestGetUser(t, func() rpc.HTTPError {
			return doRequest("path", "/a", "uploadId", uploadID)
		}, testUserID)
		node.OnceGetUser()
		node.Volume.EXPECT().AbortMultiPart(A, A, A).Return(e1)
		require.Equal(t, e1.Status, doRequest("path", "/a", "uploadId", uploadID).StatusCode())
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().AbortMultiPart(A, A, A).Return(nil)
		require.NoError(t, doRequest("path", "/a", "uploadId", uploadID))
	}
}
