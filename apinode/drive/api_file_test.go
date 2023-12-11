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
	"archive/tar"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/proto"
)

func TestHandleFileUpload(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(body *mockBody, queries ...string) *http.Response {
		url := genURL(server.URL, "/v1/files/upload", queries...)
		req, _ := http.NewRequest(http.MethodPut, url, body)
		req.Header.Add(HeaderUserID, testUserID.ID)
		req.Header.Add(HeaderCrc32, fmt.Sprint(body.Sum32()))
		req.Header.Add(EncodeMetaHeader("upload"), EncodeMeta("Uploaded-"))
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
		node.TestGetUser(t, func() rpc.HTTPError {
			resp := doRequest(newMockBody(64), "path", "/f")
			defer resp.Body.Close()
			return resp2Error(resp)
		}, testUserID)
		node.OnceGetUser()
		// create dir error
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		resp := doRequest(newMockBody(64), "path", "/dir/a/../filename")
		defer resp.Body.Close()
		require.Equal(t, e1.Status, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		resp := doRequest(newMockBody(64), "path", "/a", "fileId", "1111")
		defer resp.Body.Close()
		require.Equal(t, sdk.ErrConflict.Status, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		resp := doRequest(newMockBody(64), "path", "/a", "fileId", "1111")
		defer resp.Body.Close()
		require.Equal(t, e1.Status, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(&sdk.DirInfo{Inode: 1000}, nil)
		resp := doRequest(newMockBody(64), "path", "/a", "fileId", "1111")
		defer resp.Body.Close()
		require.Equal(t, sdk.ErrConflict.Status, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		// invalid crc
		node.Volume.EXPECT().Lookup(A, A, A).Return(&sdk.DirInfo{Inode: 1000}, nil)
		url := genURL(server.URL, "/v1/files/upload", "path", "/dir/a/../filename")
		req, _ := http.NewRequest(http.MethodPost, url, newMockBody(64))
		req.Header.Add(HeaderUserID, testUserID.ID)
		req.Header.Add(HeaderCrc32, "invalid-crc-32")
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		node.Volume.EXPECT().Lookup(A, A, A).Return(&sdk.DirInfo{Inode: 1000}, nil)
		node.Volume.EXPECT().UploadFile(A, A).Return(nil, uint64(0), e2)
		// uploda file error
		resp := doRequest(newMockBody(64), "path", "/dir/a/../filename")
		defer resp.Body.Close()
		require.Equal(t, e2.Status, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		node.Volume.EXPECT().Lookup(A, A, A).Return(&sdk.DirInfo{Inode: 1000}, nil)
		node.Volume.EXPECT().UploadFile(A, A).DoAndReturn(
			func(_ context.Context, req *sdk.UploadFileReq) (*sdk.InodeInfo, uint64, error) {
				req.Callback()
				return &sdk.InodeInfo{Inode: node.GenInode()}, uint64(100), nil
			})
		resp := doRequest(newMockBody(64), "path", "/dir/a/../filename")
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)

		buff, _ := io.ReadAll(resp.Body)
		var file FileInfo
		require.NoError(t, json.Unmarshal(buff, &file))
		require.Equal(t, "filename", file.Name)
		require.Equal(t, "Uploaded-", file.Properties["upload"])
		require.Equal(t, 32, len(file.Properties[internalMetaMD5]))
	}
}

func TestHandleFileBatchUpload(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(body *bytes.Buffer, data interface{}) rpc.HTTPError {
		url := genURL(server.URL, "/v1/files/uploads")
		req, _ := http.NewRequest(http.MethodPost, url, body)
		req.Header.Add(HeaderUserID, testUserID.ID)
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		return resp2Data(resp, data)
	}

	buf := new(bytes.Buffer)
	var res BatchUploadFileResult
	hasError := func(status int) {
		require.Equal(t, 1, len(res.Errors))
		require.Equal(t, status, res.Errors[0].Status, res.Errors[0].Message)
	}
	{
		node.TestGetUser(t, func() rpc.HTTPError {
			return doRequest(buf, nil)
		}, testUserID)
		node.OnceGetUser()
		resp := doRequest(buf, &res)
		require.NoError(t, resp)
		require.Equal(t, 0, len(res.Uploaded))
	}
	{
		buf.Write(newMockBody(54).buff)
		node.OnceGetUser()
		require.NoError(t, doRequest(buf, &res))
		hasError(400)
		buf.Reset()
	}
	{
		for _, hdr := range []tar.Header{
			{Name: ""},
			{Name: "../../file"},
			{Name: "/file", Format: tar.FormatPAX, PAXRecords: map[string]string{PAXFileID: "x32"}},
		} {
			node.OnceGetUser()
			tw := tar.NewWriter(buf)
			require.NoError(t, tw.WriteHeader(&hdr))
			require.NoError(t, doRequest(buf, &res))
			hasError(400)
			buf.Reset()
		}
	}

	body := newMockBody(64)
	hdr := &tar.Header{
		Name:   "/file",
		Size:   int64(len(body.buff)),
		Format: tar.FormatPAX,
		PAXRecords: map[string]string{
			PAXCrc32: fmt.Sprintf("%d", body.Sum32()),
		},
	}
	writeBuff := func() {
		tw := tar.NewWriter(buf)
		require.NoError(t, tw.WriteHeader(hdr))
		_, err := tw.Write(body.buff[:])
		require.NoError(t, err)
	}
	{
		node.OnceGetUser()
		hdr.Name = "/it's/a/../a/file"
		writeBuff()
		// create dir error
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		require.NoError(t, doRequest(buf, &res))
		hasError(e1.Status)
		hdr.Name = "/file"
		buf.Reset()
	}
	{
		node.OnceGetUser()
		hdr.PAXRecords[PAXFileID] = "1000"
		writeBuff()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e2)
		require.NoError(t, doRequest(buf, &res))
		hasError(e2.Status)
		buf.Reset()
	}
	node.Volume.EXPECT().Lookup(A, A, A).Return(&sdk.DirInfo{Inode: 1000, FileId: 1000}, nil).AnyTimes()
	{
		node.OnceGetUser()
		hdr.PAXRecords[PAXCrc32] = "not-number"
		writeBuff()
		require.NoError(t, doRequest(buf, &res))
		hasError(400)
		buf.Reset()
	}
	hdr.PAXRecords = map[string]string{
		PAXFileID: "1000",
		PAXCrc32:  fmt.Sprintf("%d", body.Sum32()),
	}
	{
		hdr.PAXRecords[UserPropertyPrefix+internalMetaMD5] = "internal"
		node.OnceGetUser()
		writeBuff()
		require.NoError(t, doRequest(buf, &res))
		hasError(400)
		buf.Reset()
		delete(hdr.PAXRecords, UserPropertyPrefix+internalMetaMD5)
	}
	{
		node.OnceGetUser()
		writeBuff()
		node.Volume.EXPECT().UploadFile(A, A).Return(nil, uint64(0), e3)
		require.NoError(t, doRequest(buf, &res))
		hasError(e3.Status)
		buf.Reset()
	}
	{
		hdr.PAXRecords[PAXMD5] = "md5"
		node.OnceGetUser()
		writeBuff()
		node.Volume.EXPECT().UploadFile(A, A).DoAndReturn(
			func(_ context.Context, req *sdk.UploadFileReq) (*sdk.InodeInfo, uint64, error) {
				err := req.Callback()
				return &sdk.InodeInfo{Inode: node.GenInode()}, uint64(100), err
			})
		require.NoError(t, doRequest(buf, &res))
		hasError(sdk.ErrMismatchChecksum.Status)
		buf.Reset()
	}
	{
		hdr.PAXRecords[PAXMD5] = "md5"
		tw := tar.NewWriter(buf)
		require.NoError(t, tw.WriteHeader(hdr))
		_, err := tw.Write(body.buff[:])
		require.NoError(t, err)
		require.NoError(t, tw.WriteHeader(hdr))
		_, err = tw.Write(body.buff[:])
		require.NoError(t, err)
		delete(hdr.PAXRecords, PAXMD5)
		require.NoError(t, tw.WriteHeader(hdr))
		_, err = tw.Write(body.buff[:])
		require.NoError(t, err)

		node.OnceGetUser()
		node.Volume.EXPECT().UploadFile(A, A).DoAndReturn(
			func(_ context.Context, req *sdk.UploadFileReq) (*sdk.InodeInfo, uint64, error) {
				err := req.Callback()
				return &sdk.InodeInfo{Inode: node.GenInode()}, uint64(100), err
			}).Times(3)
		require.NoError(t, doRequest(buf, &res))
		require.Equal(t, 1, len(res.Uploaded))
		require.Equal(t, 2, len(res.Errors))
		require.Equal(t, sdk.ErrMismatchChecksum.Status, res.Errors[1].Status, res.Errors[1].Message)
		buf.Reset()
	}
	hasher := md5.New()
	hasher.Write(body.buff)
	hdr.PAXRecords[PAXMD5] = hex.EncodeToString(hasher.Sum(nil))
	{
		chinese := "中文-key"
		hdr.PAXRecords[UserPropertyPrefix+"key"] = chinese

		tw := tar.NewWriter(buf)
		require.NoError(t, tw.WriteHeader(hdr))
		_, err := tw.Write(body.buff[:])
		require.NoError(t, err)
		require.NoError(t, tw.WriteHeader(hdr))
		_, err = tw.Write(body.buff[:])
		require.NoError(t, err)

		node.OnceGetUser()
		node.Volume.EXPECT().UploadFile(A, A).DoAndReturn(
			func(_ context.Context, req *sdk.UploadFileReq) (*sdk.InodeInfo, uint64, error) {
				_, err := io.CopyN(io.Discard, req.Body, int64(len(body.buff)))
				require.NoError(t, err)
				err = req.Callback()
				return &sdk.InodeInfo{Inode: node.GenInode()}, uint64(100), err
			}).Times(2)
		require.NoError(t, doRequest(buf, &res))
		require.Equal(t, 0, len(res.Errors))
		require.Equal(t, 2, len(res.Uploaded))
		require.Equal(t, "/file", res.Uploaded[0].Name)
		require.Equal(t, chinese, res.Uploaded[1].Properties["key"])
		buf.Reset()
	}
}

func TestHandleFileUploadPublic(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(body *mockBody, queries ...string) *http.Response {
		url := genURL(server.URL, "/v1/files/upload", queries...)
		req, _ := http.NewRequest(http.MethodPut, url, body)
		req.Header.Add(HeaderUserID, testUserID.ID)
		req.Header.Add(HeaderCrc32, fmt.Sprint(body.Sum32()))
		req.Header.Add(EncodeMetaHeader("public"), EncodeMeta("Public-"))
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		return resp
	}
	{
		node.TestGetUser(t, func() rpc.HTTPError {
			resp := doRequest(newMockBody(64), "path", "/f")
			defer resp.Body.Close()
			return resp2Error(resp)
		}, testUserID, testUserAPP)
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		node.Volume.EXPECT().UploadFile(A, A).DoAndReturn(
			func(_ context.Context, req *sdk.UploadFileReq) (*sdk.InodeInfo, uint64, error) {
				req.Callback()
				return &sdk.InodeInfo{Inode: node.GenInode()}, uint64(100), nil
			})
		path := fmt.Sprintf("/%s/%s/a/../publicfile", testUserAPP.ID, publicFolder)
		resp := doRequest(newMockBody(64), "path", path)
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)

		buff, _ := io.ReadAll(resp.Body)
		var file FileInfo
		require.NoError(t, json.Unmarshal(buff, &file))
		require.Equal(t, "publicfile", file.Name)
		require.Equal(t, "Public-", file.Properties["public"])
		require.Equal(t, 32, len(file.Properties[internalMetaMD5]))
	}
}

func TestHandleFileVerify(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(ranged string, md5 string, paths ...string) rpc.HTTPError {
		p := "/verify"
		if len(paths) > 0 {
			p = paths[0]
		}
		url := genURL(server.URL, "/v1/files/verify", "path", p)
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		req.Header.Add(HeaderUserID, testUserID.ID)
		if ranged != "" {
			req.Header.Add(headerRange, ranged)
		}
		if md5 != "" {
			req.Header.Set(ChecksumPrefix+"md5", md5)
		}
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		return resp2Error(resp)
	}

	{
		require.Equal(t, 400, doRequest("", "", "").StatusCode())
		require.Equal(t, 400, doRequest("", "", "../a").StatusCode())
	}
	{
		node.TestGetUser(t, func() rpc.HTTPError {
			return doRequest("", "")
		}, testUserID)
		require.Equal(t, 400, doRequest("", "", "").StatusCode())
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		require.Equal(t, e1.Status, doRequest("", "", "/a").StatusCode())
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		require.Equal(t, sdk.ErrNotFile.Status, doRequest("", "", "/a").StatusCode())
	}
	{
		node.OnceGetUser()
		node.OnceLookup(false)
		node.Volume.EXPECT().GetInode(A, A).Return(nil, e2)
		require.Equal(t, e2.Status, doRequest("", "", "/a").StatusCode())
	}
	{
		node.OnceGetUser()
		node.OnceLookup(false)
		node.OnceGetInode()
		require.Equal(t, 400, doRequest("bytes=10240-", "").StatusCode())
	}
	{
		node.OnceGetUser()
		node.OnceLookup(false)
		node.OnceGetInode()
		body := &mockBody{buff: []byte("checksums")}
		node.Volume.EXPECT().ReadFile(A, A, A, A).DoAndReturn(
			func(_ context.Context, _, _ uint64, p []byte) (int, error) {
				return body.Read(p)
			})
		require.Equal(t, sdk.ErrMismatchChecksum.Status,
			doRequest("bytes=0-8", "42b8d9faf974b929ed89a56df591d9a0").StatusCode())
	}
	{
		node.OnceGetUser()
		node.OnceLookup(false)
		node.Volume.EXPECT().GetInode(A, A).Return(&proto.InodeInfo{Size: 0}, nil)
		require.NoError(t, doRequest("", ""))
	}
	{
		node.OnceGetUser()
		node.OnceLookup(false)
		node.OnceGetInode()
		body := &mockBody{buff: []byte("checksums")}
		node.Volume.EXPECT().ReadFile(A, A, A, A).DoAndReturn(
			func(_ context.Context, _, _ uint64, p []byte) (int, error) {
				return body.Read(p)
			})
		require.NoError(t, doRequest("bytes=0-8", "42b8d9faf974b929ed89a56df591d9a7"))
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
		req.Header.Add(HeaderUserID, testUserID.ID)
		req.Header.Add(HeaderCrc32, fmt.Sprint(body.Sum32()))
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
		node.TestGetUser(t, func() rpc.HTTPError {
			resp := doRequest(newMockBody(64), "", queries...)
			defer resp.Body.Close()
			return resp2Error(resp)
		}, testUserID)
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		resp := doRequest(newMockBody(64), "", queries...)
		defer resp.Body.Close()
		require.Equal(t, sdk.ErrConflict.Status, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		resp := doRequest(newMockBody(64), "", queries...)
		defer resp.Body.Close()
		require.Equal(t, sdk.ErrNotFile.Status, resp.StatusCode)
	}
	node.Volume.EXPECT().Lookup(A, A, A).Return(&sdk.DirInfo{Inode: 1111, FileId: 1111}, nil).AnyTimes()
	{
		node.OnceGetUser()
		node.Volume.EXPECT().GetInode(A, A).Return(nil, e1)
		resp := doRequest(newMockBody(64), "", queries...)
		defer resp.Body.Close()
		require.Equal(t, e1.Status, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().GetInode(A, A).Return(&proto.InodeInfo{Size: 1024}, nil)
		resp := doRequest(newMockBody(64), "bytes=i-j", queries...)
		defer resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().GetInode(A, A).Return(&proto.InodeInfo{Size: 1024}, nil)
		resp := doRequest(newMockBody(1), "bytes=1-", queries...)
		defer resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().GetInode(A, A).Return(&proto.InodeInfo{Size: 1024}, nil)
		resp := doRequest(newMockBody(64), "bytes=1025-", queries...)
		defer resp.Body.Close()
		require.Equal(t, sdk.ErrWriteOverSize.Status, resp.StatusCode)
	}
	{
		file = newMockBody(1024)
		node.OnceGetUser()
		node.Volume.EXPECT().GetInode(A, A).Return(&proto.InodeInfo{Size: 1024}, nil)
		node.Volume.EXPECT().DeleteXAttr(A, A, A).Return(e2)
		resp := doRequest(newMockBody(64), "bytes=1024-", queries...)
		defer resp.Body.Close()
		require.Equal(t, e2.Status, resp.StatusCode)
	}
	node.Volume.EXPECT().DeleteXAttr(A, A, A).Return(nil).AnyTimes()
	{
		file = newMockBody(1024)
		node.OnceGetUser()
		node.Volume.EXPECT().GetInode(A, A).Return(&proto.InodeInfo{Size: 1024}, nil)
		node.Volume.EXPECT().WriteFile(A, A, A, A, A).Return(nil)
		resp := doRequest(newMockBody(64), "bytes=100-", queries...)
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().GetInode(A, A).Return(&proto.InodeInfo{Size: 1024}, nil)
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
	{
		node.OnceGetUser()
		resp := doRequest(newMockBody(64), "", "path", "/a", "fileId", "2222")
		defer resp.Body.Close()
		require.Equal(t, sdk.ErrConflict.Status, resp.StatusCode)
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
		req.Header.Add(HeaderUserID, testUserID.ID)
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
		node.TestGetUser(t, func() rpc.HTTPError {
			resp := doRequest(newMockBody(64), "", "path", "/download")
			defer resp.Body.Close()
			return resp2Error(resp)
		}, testUserID)
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		resp := doRequest(newMockBody(64), "", "path", "/download")
		defer resp.Body.Close()
		require.Equal(t, e1.Status, resp.StatusCode)
	}
	node.Volume.EXPECT().GetXAttr(A, A, A).Return("", nil).AnyTimes()
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
		node.OnceGetUser()
		node.OnceLookup(false)
		node.Volume.EXPECT().GetInode(A, A).Return(&proto.InodeInfo{Size: 0}, nil)
		resp := doRequest(nil, "", "path", "/download")
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)
		buff, _ := io.ReadAll(resp.Body)
		require.Equal(t, 0, len(buff))
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
		node.OnceGetUser()
		node.OnceLookup(false)
		node.OnceGetInode()
		resp := doRequest(nil, "bytes=1024-10000", "path", "/download")
		defer resp.Body.Close()
		require.Equal(t, 206, resp.StatusCode)
		buff, _ := io.ReadAll(resp.Body)
		require.Equal(t, 0, len(buff))
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
		resp := doRequest(nil, "bytes=996-", "path", "/download")
		defer resp.Body.Close()
		require.Equal(t, 206, resp.StatusCode)
		buff, _ := io.ReadAll(resp.Body)
		require.Equal(t, origin, buff)
	}
	{
		size := 1024
		node.OnceGetUser()
		node.OnceLookup(false)
		node.OnceGetInode()
		body := newMockBody(size)
		hasher := md5.New()
		hasher.Write(body.buff[:size])
		md5sum := hex.EncodeToString(hasher.Sum(nil))
		node.Volume.EXPECT().ReadFile(A, A, A, A).DoAndReturn(
			func(_ context.Context, _, _ uint64, p []byte) (int, error) {
				return body.Read(p)
			}).Times(2)
		node.Volume.EXPECT().SetXAttr(A, A, A, A).DoAndReturn(
			func(_ context.Context, _ uint64, key, val string) error {
				require.Equal(t, internalMetaMD5, key)
				require.Equal(t, md5sum, val)
				return nil
			})
		resp := doRequest(nil, "bytes=0-", "path", "/download")
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)
		buff, _ := io.ReadAll(resp.Body)
		require.Equal(t, 1024, len(buff))
	}
}

func TestHandleFileBatchDownload(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(body []byte) *http.Response {
		url := genURL(server.URL, "/v1/files/contents")
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		req.Header.Add(HeaderUserID, testUserID.ID)
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		return resp
	}

	{
		resp := doRequest([]byte{'}'})
		resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)
		buff, _ := json.Marshal([]string{"/a", "?"})
		resp = doRequest(buff)
		resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)
	}
	{
		node.TestGetUser(t, func() rpc.HTTPError {
			resp := doRequest([]byte{'[', ']'})
			defer resp.Body.Close()
			return resp2Error(resp)
		}, testUserID)
	}
	body, _ := json.Marshal([]string{"/a", "/b", "/c"})
	{
		node.OnceGetUser()
		node.LookupDirN(3)
		resp := doRequest(body)
		resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)
	}
	{
		node.OnceGetUser()
		node.LookupN(3)
		node.Volume.EXPECT().GetInode(A, A).Return(nil, e1).Times(3)
		resp := doRequest(body)
		buff, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
		tr := tar.NewReader(bytes.NewReader(buff))
		for range [3]struct{}{} {
			hdr, err := tr.Next()
			require.NoError(t, err)
			require.Equal(t, int64(0), hdr.Size)
			require.Equal(t, fmt.Sprintf("%d", e1.Status), hdr.PAXRecords[PAXDownloadStatus])
			require.Equal(t, e1.ErrorCode(), hdr.PAXRecords[PAXDownloadCode])
			require.Equal(t, e1.Error(), hdr.PAXRecords[PAXDownloadError])
		}
	}
	{
		size := 1024
		node.OnceGetUser()
		node.LookupN(3)
		node.OnceGetInode()
		node.OnceGetInode()
		node.Volume.EXPECT().GetInode(A, A).DoAndReturn(
			func(_ context.Context, ino uint64) (*proto.InodeInfo, error) {
				return &proto.InodeInfo{
					Size:  0,
					Inode: ino,
				}, nil
			})
		bodya := newMockBody(size)
		bodyb := newMockBody(size)
		node.Volume.EXPECT().ReadFile(A, A, A, A).DoAndReturn(
			func(_ context.Context, _, _ uint64, p []byte) (int, error) {
				return bodya.Read(p)
			})
		node.Volume.EXPECT().ReadFile(A, A, A, A).DoAndReturn(
			func(_ context.Context, _, _ uint64, p []byte) (int, error) {
				return bodyb.Read(p)
			})
		resp := doRequest(body)
		defer resp.Body.Close()

		tr := tar.NewReader(resp.Body)
		for range [3]struct{}{} {
			hdr, err := tr.Next()
			require.NoError(t, err)
			if hdr.Size > 0 {
				buff, err := io.ReadAll(tr)
				require.NoError(t, err)
				require.Equal(t, size, len(buff))
			}
		}
	}
	{
		const n = 100
		node.OnceGetUser()
		node.LookupN(n)
		node.Volume.EXPECT().GetInode(A, A).DoAndReturn(
			func(_ context.Context, ino uint64) (*proto.InodeInfo, error) {
				return &proto.InodeInfo{
					Size:  0,
					Inode: ino,
				}, nil
			}).Times(n)

		var files []string
		for range [n]struct{}{} {
			files = append(files, "/a")
		}
		reqBody, _ := json.Marshal(files)
		resp := doRequest(reqBody)
		defer resp.Body.Close()

		tr := tar.NewReader(resp.Body)
		for range [n]struct{}{} {
			hdr, err := tr.Next()
			require.NoError(t, err)
			require.Equal(t, int64(0), hdr.Size)
			require.Equal(t, 0, len(hdr.PAXRecords))
		}
	}
}

func TestHandleFileDownloadConfig(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func() *http.Response {
		url := genURL(server.URL, "/v1/files/content", "path", volumeConfigPath)
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		req.Header.Add(HeaderUserID, testUserID.ID)
		req.Header.Add(HeaderVolume, "default")
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		return resp
	}
	{
		node.OnceLookup(true)
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		resp := doRequest()
		defer resp.Body.Close()
		require.Equal(t, e1.Status, resp.StatusCode)
	}
	{
		node.OnceLookup(true)
		node.OnceLookup(false)
		node.Volume.EXPECT().GetInode(A, A).Return(nil, e2)
		resp := doRequest()
		defer resp.Body.Close()
		require.Equal(t, e2.Status, resp.StatusCode)
	}
	{
		node.OnceLookup(true)
		node.OnceLookup(false)
		node.OnceGetInode()
		body := newMockBody(1024)
		origin := body.buff[:]
		node.Volume.EXPECT().ReadFile(A, A, A, A).DoAndReturn(
			func(_ context.Context, _, _ uint64, p []byte) (int, error) {
				return body.Read(p)
			}).AnyTimes()
		resp := doRequest()
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)
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
		req.Header.Add(HeaderUserID, testUserID.ID)
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		return resp2Error(resp)
	}

	{
		require.Equal(t, 400, doRequest("src", "/a").StatusCode())
		require.Equal(t, 400, doRequest("src", "/a", "dst", "a/b/../../..").StatusCode())
		require.Equal(t, sdk.ErrForbidden.Status, doRequest("src", "/dir/"+publicFolder+"/a", "dst", "/dir/b").StatusCode())
		require.Equal(t, sdk.ErrForbidden.Status, doRequest("src", "/u1/"+publicFolder+"/a", "dst", "/u2/"+publicFolder+"/b").StatusCode())
	}
	{
		node.ClusterMgr.EXPECT().GetCluster(A).Return(nil)
		node.TestGetUser(t, func() rpc.HTTPError {
			return doRequest("src", "/dir/a", "dst", "/dir/b")
		}, testUserID)
		node.GetUserN2()
		require.Equal(t, 400, doRequest("src", "/dir/a", "dst", "/").StatusCode())
		node.GetUserN2()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		require.Equal(t, e1.Status, doRequest("src", "/dir/a", "dst", "/dir/b").StatusCode())
	}
	{
		node.GetUserN2()
		node.OnceLookup(true)
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e2)
		require.Equal(t, e2.Status, doRequest("src", "/dir/a", "dst", "/dir/b").StatusCode())
	}
	node.GetUserAny()
	{
		node.LookupDirN(3)
		require.Equal(t, sdk.ErrConflict.Status, doRequest("src", "/dir/a/", "dst", "/dir/b/").StatusCode())
	}
	{
		node.OnceLookup(true)
		node.OnceLookup(true)
		node.OnceLookup(false)
		require.Equal(t, sdk.ErrConflict.Status, doRequest("src", "/dir/a/", "dst", "/dir/b/").StatusCode())
	}
	{
		node.OnceLookup(true)
		node.OnceLookup(true)
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		require.Equal(t, e1.Status, doRequest("src", "/dir/a/", "dst", "/dir/b/").StatusCode())
	}
	{
		node.Volume.EXPECT().Lookup(A, A, A).Return(&sdk.DirInfo{Inode: 11111}, nil).Times(2)
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		require.Equal(t, sdk.ErrForbidden.Status, doRequest("src", "/dir/a", "dst", "/dir/a").StatusCode())
	}
	{
		node.OnceLookup(true)
		node.OnceLookup(true)
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		node.Volume.EXPECT().Rename(A, A, A).Return(e3)
		require.Equal(t, e3.Status, doRequest("src", "/dir/a/", "dst", "/dir/b/").StatusCode())
	}
	for _, cs := range []struct {
		lookup   int
		src, dst string
	}{
		{lookup: 2, src: "/dir/a", dst: "/dir/b"},
		{lookup: 1, src: "/dir/a", dst: "/b"},
		{lookup: 1, src: "/a", dst: "/dir/b"},
		{lookup: 0, src: "/a", dst: "/b"},
	} {
		for i := 0; i < cs.lookup; i++ {
			node.OnceLookup(true)
		}
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		node.Volume.EXPECT().Rename(A, A, A).Return(nil)
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
		req.Header.Add(HeaderUserID, testUserID.ID)
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		return resp2Error(resp)
	}

	{
		require.Equal(t, 400, doRequest("src", "/a").StatusCode())
		require.Equal(t, 400, doRequest("src", "/a", "dst", "a/b/../../..").StatusCode())
		require.Equal(t, 400, doRequest("src", "/a", "dst", "/").StatusCode())
	}
	{
		node.ClusterMgr.EXPECT().GetCluster(A).Return(nil)
		node.TestGetUser(t, func() rpc.HTTPError {
			return doRequest("src", "/dir/a", "dst", "/dir/b")
		}, testUserID)
		node.GetUserN2()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		require.Equal(t, e1.Status, doRequest("src", "/dir/a", "dst", "/dir/b").StatusCode())
		require.Equal(t, sdk.ErrForbidden.Status, doRequest("src", "/dir/"+publicFolder+"/a", "dst", "/dir/b").StatusCode())
		require.Equal(t, sdk.ErrForbidden.Status, doRequest("src", "/u1/"+publicFolder+"/a", "dst", "/u2/"+publicFolder+"/b").StatusCode())
	}
	node.GetUserAny()
	{
		node.OnceLookup(true)
		node.OnceLookup(true)
		require.Equal(t, sdk.ErrNotFile.Status, doRequest("src", "/dir/a", "dst", "/dir/b").StatusCode())
	}
	{
		node.LookupN(2)
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		require.Equal(t, e1.Status, doRequest("src", "/dir/a", "dst", "/dir/b").StatusCode())
	}
	{
		node.LookupN(2)
		node.OnceLookup(true)
		require.Equal(t, sdk.ErrNotFile.Status, doRequest("src", "/dir/a", "dst", "/b").StatusCode())
	}
	{
		node.LookupN(3)
		node.Volume.EXPECT().GetInode(A, A).Return(nil, e2)
		require.Equal(t, e2.Status, doRequest("src", "/dir/a", "dst", "/b").StatusCode())
	}
	{
		node.LookupN(3)
		node.OnceGetInode()
		node.Volume.EXPECT().GetXAttrMap(A, A).Return(nil, e1)
		require.Equal(t, e1.Status, doRequest("src", "/dir/a", "dst", "/b", "meta", "1").StatusCode())
	}
	{
		node.LookupN(3)
		node.OnceGetInode()
		node.Volume.EXPECT().GetXAttrMap(A, A).Return(map[string]string{
			internalMetaMD5: "err-md5", "key": "value",
		}, nil)
		node.Volume.EXPECT().UploadFile(A, A).Return(nil, uint64(0), e4)
		require.Equal(t, e4.Status, doRequest("src", "/dir/a", "dst", "/b").StatusCode())
	}
	{
		node.LookupN(3)
		node.OnceGetInode()
		node.Volume.EXPECT().GetXAttrMap(A, A).Return(map[string]string{
			internalMetaMD5: "err-md5", "key": "value",
		}, nil)
		node.Volume.EXPECT().UploadFile(A, A).DoAndReturn(
			func(_ context.Context, req *sdk.UploadFileReq) (*sdk.InodeInfo, uint64, error) {
				req.Callback()
				return &sdk.InodeInfo{}, uint64(100), nil
			})
		require.NoError(t, doRequest("src", "/dir/a", "dst", "/b"))
	}
	{
		node.LookupN(3)
		node.OnceGetInode()
		node.Volume.EXPECT().GetXAttrMap(A, A).Return(map[string]string{
			internalMetaMD5: "err-md5", "key": "value",
		}, nil)
		node.Volume.EXPECT().UploadFile(A, A).Return(nil, uint64(0), e2)
		require.Equal(t, e2.Status, doRequest("src", "/dir/a", "dst", "/b").StatusCode())
	}
	{
		node.LookupN(2)
		node.OnceLookup(true)
		node.Volume.EXPECT().Lookup(A, A, A).Return(&sdk.DirInfo{Inode: 11111}, nil)
		node.OnceGetInode()
		node.Volume.EXPECT().GetXAttrMap(A, A).Return(map[string]string{
			internalMetaMD5: "err-md5", "key": "value",
		}, nil)
		node.Volume.EXPECT().UploadFile(A, A).DoAndReturn(
			func(_ context.Context, req *sdk.UploadFileReq) (*sdk.InodeInfo, uint64, error) {
				req.Callback()
				if req.OldFileId != 11111 ||
					req.Extend["internalMetaMD5"] == "err-md5" ||
					req.Extend["key"] != "value" {
					return nil, uint64(0), e3
				}
				return &sdk.InodeInfo{Inode: node.GenInode()}, uint64(0), nil
			})
		require.NoError(t, doRequest("src", "/dir/a", "dst", "/dir/b", "meta", "1"))
	}
}
