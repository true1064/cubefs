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
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/apinode/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/proto"
)

func TestFilterBuilder(t *testing.T) {
	var (
		builders []filterBuilder
		err      error
	)

	_, err = makeFilterBuilders("name=")
	require.NotNil(t, err)

	_, err = makeFilterBuilders("name=12345")
	require.NotNil(t, err)

	builders, err = makeFilterBuilders("name = 12345")
	require.Nil(t, err)
	require.Equal(t, 1, len(builders))
	ok := builders[0].match("12345")
	require.True(t, ok)

	ok = builders[0].match("123")
	require.False(t, ok)
	ok = builders[0].match("123456")
	require.False(t, ok)

	_, err = makeFilterBuilders("name = 12345;type = ")
	require.NotNil(t, err)
	_, err = makeFilterBuilders("name = 12345;type = fil")
	require.NotNil(t, err)
	_, err = makeFilterBuilders("name = 12345;type = *\\.doc")
	require.NotNil(t, err)

	builders, err = makeFilterBuilders("name = 12345;type = file")
	require.NoError(t, err)
	require.Equal(t, 2, len(builders))
	require.True(t, builders[0].match("12345"))
	require.True(t, builders[1].match("file"))

	builders, err = makeFilterBuilders("name != 12345;type = file")
	require.NoError(t, err)
	require.Equal(t, 2, len(builders))
	require.False(t, builders[0].match("12345"))
	require.True(t, builders[1].match("file"))

	builders, err = makeFilterBuilders("name contains (.*)\\.doc$;type = file")
	require.NoError(t, err)
	require.Equal(t, 2, len(builders))
	require.True(t, builders[0].match("12.doc"))
	require.True(t, builders[0].match("12345.doc"))
	require.False(t, builders[0].match("doc"))
	require.False(t, builders[0].match("adoc"))
	require.False(t, builders[0].match("345.doc12"))
	require.True(t, builders[1].match("file"))

	builders, err = makeFilterBuilders("name contains (.*)\\.doc$;type = file;propertyKey = 12345")
	require.NoError(t, err)
	require.Equal(t, 3, len(builders))
	require.True(t, builders[2].match("12345"))
	require.False(t, builders[2].match("1234"))
}

func TestHandleListDir(t *testing.T) {
	ctrl := gomock.NewController(t)
	urm, _ := NewUserRouteMgr()
	mockCluster := mocks.NewMockICluster(ctrl)
	mockVol := mocks.NewMockIVolume(ctrl)
	mockClusterMgr := mocks.NewMockClusterManager(ctrl)
	d := &DriveNode{
		vol:        mockVol,
		userRouter: urm,
		cryptor:    newMockCryptor(t),
		clusterMgr: mockClusterMgr,
	}
	ts := httptest.NewServer(d.RegisterAPIRouters())
	defer ts.Close()

	testUser := UserID{ID: "test"}
	client := ts.Client()
	{
		tgt := fmt.Sprintf("%s/v1/files", ts.URL)
		req, err := http.NewRequest(http.MethodGet, tgt, nil)
		require.NoError(t, err)
		res, err := client.Do(req)
		require.NoError(t, err)
		defer res.Body.Close()
		require.Equal(t, res.StatusCode, http.StatusBadRequest)
	}

	{
		tgt := fmt.Sprintf("%s/v1/files?path=%s&limit=10", ts.URL, url.QueryEscape("/test"))
		req, err := http.NewRequest(http.MethodGet, tgt, nil)
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		defer res.Body.Close()
		require.Equal(t, res.StatusCode, http.StatusBadRequest) // no uid
	}

	{
		// getRootInoAndVolume error
		mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("not found"))
		tgt := fmt.Sprintf("%s/v1/files?path=%s&limit=10", ts.URL, url.QueryEscape("/test"))
		req, err := http.NewRequest(http.MethodGet, tgt, nil)
		require.Nil(t, err)
		req.Header.Set(HeaderUserID, "test")
		res, err := client.Do(req)
		require.Nil(t, err)
		defer res.Body.Close()
		require.Equal(t, res.StatusCode, http.StatusInternalServerError)
	}

	{
		urm.Set(testUser, &UserRoute{
			Uid:        testUser.ID,
			Public:     testUser.Public,
			ClusterID:  "1",
			VolumeID:   "1",
			RootPath:   getRootPath(testUser),
			RootFileID: 4,
		})

		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(mockCluster)
		mockCluster.EXPECT().GetVol(gomock.Any()).Return(mockVol)
		mockVol.EXPECT().GetDirSnapshot(A, A).Return(mockVol, nil)
		mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
				if name == "test" {
					return &sdk.DirInfo{
						Name:  name,
						Inode: parentIno + 1,
						Type:  uint32(os.ModeIrregular),
					}, nil
				}
				return nil, sdk.ErrNotFound
			})
		tgt := fmt.Sprintf("%s/v1/files?path=%s&limit=10", ts.URL, url.QueryEscape("/test"))
		req, err := http.NewRequest(http.MethodGet, tgt, nil)
		require.Nil(t, err)
		req.Header.Set(HeaderUserID, "test")
		res, err := client.Do(req)
		require.Nil(t, err)
		res.Body.Close()
		require.Equal(t, res.StatusCode, 452)
		urm.Remove(testUser)
	}

	{
		urm.Set(testUser, &UserRoute{
			Uid:        testUser.ID,
			Public:     testUser.Public,
			ClusterID:  "1",
			VolumeID:   "1",
			RootPath:   getRootPath(testUser),
			RootFileID: 4,
		})

		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(mockCluster)
		mockCluster.EXPECT().GetVol(gomock.Any()).Return(mockVol)
		mockVol.EXPECT().GetDirSnapshot(A, A).Return(mockVol, nil)
		mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
				if name == "test" {
					return &sdk.DirInfo{
						Name:  name,
						Inode: parentIno + 1,
						Type:  uint32(os.ModeDir),
					}, nil
				}
				return nil, sdk.ErrNotFound
			})
		mockVol.EXPECT().GetXAttrMap(gomock.Any(), gomock.Any()).Return(nil, nil)
		mockVol.EXPECT().Readdir(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, parIno uint64, marker string, count uint32) ([]sdk.DirInfo, error) {
				infos := []sdk.DirInfo{
					{Name: "123", Inode: 100, Type: uint32(os.ModeDir)},
					{Name: "234", Inode: 101, Type: uint32(os.ModeIrregular)},
				}
				return infos, nil
			})

		mockVol.EXPECT().BatchGetInodes(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, inos []uint64) ([]*proto.InodeInfo, error) {
				infos := []*proto.InodeInfo{}
				for _, ino := range inos {
					mode := uint32(os.ModeIrregular)
					if ino == 100 {
						mode = uint32(os.ModeDir)
					}
					info := &proto.InodeInfo{
						Inode:      ino,
						Size:       1024,
						Mode:       mode,
						ModifyTime: time.Now(),
						CreateTime: time.Now(),
						AccessTime: time.Now(),
					}
					infos = append(infos, info)
				}
				return infos, nil
			})
		mockVol.EXPECT().GetXAttrMap(gomock.Any(), gomock.Any()).Return(nil, nil).Times(2)
		tgt := fmt.Sprintf("%s/v1/files?path=%s&limit=10", ts.URL, url.QueryEscape("/test"))
		req, err := http.NewRequest(http.MethodGet, tgt, nil)
		require.Nil(t, err)
		req.Header.Set(HeaderUserID, "test")
		res, err := client.Do(req)
		require.Nil(t, err)
		res.Body.Close()
		require.Equal(t, res.StatusCode, http.StatusOK)
		urm.Remove(testUser)
	}
}

func TestHandleListAll(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(path string, marker string, limit int) (*ListAllResult, rpc.HTTPError) {
		url := genURL(server.URL, "/v1/files/recursive", "path", path, "marker", marker, "limit", fmt.Sprintf("%d", limit))
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		req.Header.Add(HeaderRequestID, "user_request_id")
		req.Header.Add(HeaderUserID, testUserID)
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		if resp.StatusCode != http.StatusOK {
			return nil, rpc.ParseResponseErr(resp).(rpc.HTTPError)
		}
		defer resp.Body.Close()
		result := &ListAllResult{}
		err1 := resp2Data(resp, &result)
		return result, err1
	}

	{
		node.OnceGetUser(testUserID)
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
		_, err := doRequest("/test", "", 0)
		require.Equal(t, 404, err.StatusCode())
	}

	{
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(&sdk.DirInfo{
			Inode: 100,
			Type:  uint32(fs.ModeAppend),
		}, nil)
		_, err := doRequest("/test", "", 0)
		require.Equal(t, sdk.ErrNotDir.StatusCode(), err.StatusCode())
	}

	{
		inoMap := map[uint64]string{}
		for i := 0; i < 3; i++ {
			inode := uint64((i + 1) * 100)
			inoMap[inode] = typeFolder
			for j := 1; j <= 10; j++ {
				inoMap[inode+uint64(j)] = typeFile
			}
		}
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).DoAndReturn(func(ctx context.Context, ino uint64, name string) (*sdk.DirInfo, error) {
			inode := uint64(100)
			if name == "test" {
				inode = uint64(100)
			} else {
				i, _ := strconv.Atoi(name)
				inode = uint64(i)
			}
			if _, ok := inoMap[inode]; ok {
				return &sdk.DirInfo{
					Name:  name,
					Inode: inode,
					Type:  uint32(fs.ModeDir),
				}, nil
			}
			return nil, sdk.ErrNotFound
		}).AnyTimes()
		node.Volume.EXPECT().ReadDirAll(A, A).DoAndReturn(func(ctx context.Context, ino uint64) ([]sdk.DirInfo, error) {
			dirInfos := []sdk.DirInfo{}
			if ino == 100 || ino == 200 || ino == 300 {
				for i := 1; i <= 10; i++ {
					dirInfos = append(dirInfos, sdk.DirInfo{
						Name:  fmt.Sprintf("%d", ino+uint64(i)),
						Inode: ino + uint64(i),
						Type:  uint32(fs.ModeAppend),
					})
				}
				if ino != 300 {
					dirInfos = append(dirInfos, sdk.DirInfo{
						Name:  fmt.Sprintf("%d", ino+100),
						Inode: ino + 100,
						Type:  uint32(fs.ModeDir),
					})
				}
			}
			return dirInfos, nil
		}).AnyTimes()
		node.Volume.EXPECT().GetInode(A, A).DoAndReturn(func(ctx context.Context, ino uint64) (*sdk.InodeInfo, error) {
			return &sdk.InodeInfo{
				Inode:      ino,
				Size:       1024,
				CreateTime: time.Now(),
				ModifyTime: time.Now(),
				AccessTime: time.Now(),
			}, nil
		}).AnyTimes()
		node.Volume.EXPECT().GetXAttrMap(A, A).Return(nil, nil).AnyTimes()
		result, err := doRequest("/test", "", 0)
		require.Nil(t, err)
		require.Equal(t, 32, len(result.Files))

		node.OnceGetUser()
		result, err = doRequest("/test", "200/209", 0)
		require.Nil(t, err)
		require.Equal(t, 13, len(result.Files))

		node.OnceGetUser()
		result, err = doRequest("/test", "400", 0)
		require.Nil(t, err)
		require.Equal(t, 0, len(result.Files))
	}
}
