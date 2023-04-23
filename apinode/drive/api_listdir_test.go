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
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/apinode/testing/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/singleflight"
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
	defer ctrl.Finish()

	urm, _ := NewUserRouteMgr()
	mockCluster := mocks.NewMockICluster(ctrl)
	mockVol := mocks.NewMockIVolume(ctrl)
	mockClusterMgr := mocks.NewMockClusterManager(ctrl)
	d := &DriveNode{
		vol:         mockVol,
		userRouter:  urm,
		clusterMgr:  mockClusterMgr,
		groupRouter: singleflight.Group{},
	}
	ts := httptest.NewServer(d.RegisterAPIRouters())
	defer ts.Close()

	client := ts.Client()
	{
		tgt := fmt.Sprintf("%s/v1/files", ts.URL)
		res, err := client.Get(tgt)
		require.Nil(t, err)
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
		req.Header.Set(headerUserID, "test")
		res, err := client.Do(req)
		require.Nil(t, err)
		defer res.Body.Close()
		require.Equal(t, res.StatusCode, http.StatusInternalServerError)
	}

	{
		urm.Set("test", &UserRoute{
			Uid:        UserID("test"),
			ClusterID:  "1",
			VolumeID:   "1",
			RootPath:   getRootPath("test"),
			RootFileID: 4,
		})
		defer urm.Remove("test")

		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(mockCluster)
		mockCluster.EXPECT().GetVol(gomock.Any()).Return(mockVol)
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
		req.Header.Set(headerUserID, "test")
		res, err := client.Do(req)
		require.Nil(t, err)
		defer res.Body.Close()
		require.Equal(t, res.StatusCode, 452)
	}

	{
		urm.Set("test", &UserRoute{
			Uid:        UserID("test"),
			ClusterID:  "1",
			VolumeID:   "1",
			RootPath:   getRootPath("test"),
			RootFileID: 4,
		})
		defer urm.Remove("test")

		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(mockCluster)
		mockCluster.EXPECT().GetVol(gomock.Any()).Return(mockVol)
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
			func(ctx context.Context, inos []uint64) ([]*sdk.InodeInfo, error) {
				infos := []*sdk.InodeInfo{}
				for _, ino := range inos {
					mode := uint32(os.ModeIrregular)
					if ino == 100 {
						mode = uint32(os.ModeDir)
					}
					info := &sdk.InodeInfo{
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
		req.Header.Set(headerUserID, "test")
		res, err := client.Do(req)
		require.Nil(t, err)
		defer res.Body.Close()
		require.Equal(t, res.StatusCode, http.StatusOK)
	}
}
