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

func TestHandleCreateDrive(t *testing.T) {
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
		clusters:    []string{"1", "2"},
	}
	ts := httptest.NewServer(d.RegisterAPIRouters())
	defer ts.Close()

	client := ts.Client()

	{
		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(mockCluster)
		mockCluster.EXPECT().ListVols().Return([]*sdk.VolInfo{
			&sdk.VolInfo{"1", 10},
			&sdk.VolInfo{"2", 10},
		})
		mockCluster.EXPECT().GetVol(gomock.Any()).Return(mockVol)
		mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
				inode := parentIno + 1
				if name == "usr" {
					inode = 100
				}
				fileType := uint32(os.ModeDir)
				return &sdk.DirInfo{
					Name:  name,
					Inode: inode,
					Type:  fileType,
				}, nil
			}).Times(6)
		mockVol.EXPECT().GetInode(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, ino uint64) (*sdk.InodeInfo, error) {
				return &sdk.InodeInfo{
					Inode:      ino,
					Mode:       uint32(os.ModeDir),
					Nlink:      0,
					Size:       0,
					ModifyTime: time.Now(),
					CreateTime: time.Now(),
					AccessTime: time.Now(),
				}, nil
			}).Times(2)
		mockVol.EXPECT().CreateFile(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, error) {
				return &sdk.InodeInfo{
					Inode:      parentIno + 1,
					Mode:       uint32(os.ModeIrregular),
					Nlink:      0,
					Size:       0,
					ModifyTime: time.Now(),
					CreateTime: time.Now(),
					AccessTime: time.Now(),
				}, nil
			})
		mockVol.EXPECT().SetXAttr(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

		tgt := fmt.Sprintf("%s/v1/drive", ts.URL)
		req, err := http.NewRequest(http.MethodPost, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, http.StatusOK)
		res.Body.Close()
	}

	{
		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(nil)
		tgt := fmt.Sprintf("%s/v1/drive", ts.URL)
		req, err := http.NewRequest(http.MethodPost, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, sdk.ErrNoCluster.Status)
		res.Body.Close()
	}

	{
		tmp := d.clusters
		d.clusters = []string{}
		tgt := fmt.Sprintf("%s/v1/drive", ts.URL)
		req, err := http.NewRequest(http.MethodPost, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, sdk.ErrNoCluster.Status)
		res.Body.Close()
		d.clusters = tmp
	}

	{
		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(mockCluster)
		mockCluster.EXPECT().ListVols().Return(nil)
		tgt := fmt.Sprintf("%s/v1/drive", ts.URL)
		req, err := http.NewRequest(http.MethodPost, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, sdk.ErrNoVolume.Status)
		res.Body.Close()
	}

	{
		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(mockCluster)
		mockCluster.EXPECT().ListVols().Return([]*sdk.VolInfo{
			&sdk.VolInfo{"1", 10},
			&sdk.VolInfo{"2", 10},
		})
		mockCluster.EXPECT().GetVol(gomock.Any()).Return(nil)
		tgt := fmt.Sprintf("%s/v1/drive", ts.URL)
		req, err := http.NewRequest(http.MethodPost, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, sdk.ErrNoVolume.Status)
		res.Body.Close()
	}

	{
		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(mockCluster)
		mockCluster.EXPECT().ListVols().Return([]*sdk.VolInfo{
			&sdk.VolInfo{"1", 10},
			&sdk.VolInfo{"2", 10},
		})
		mockCluster.EXPECT().GetVol(gomock.Any()).Return(mockVol)
		mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, sdk.ErrConflict)
		tgt := fmt.Sprintf("%s/v1/drive", ts.URL)
		req, err := http.NewRequest(http.MethodPost, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, sdk.ErrConflict.Status)
		res.Body.Close()
	}

	{
		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(mockCluster)
		mockCluster.EXPECT().ListVols().Return([]*sdk.VolInfo{
			&sdk.VolInfo{"1", 10},
			&sdk.VolInfo{"2", 10},
		})
		mockCluster.EXPECT().GetVol(gomock.Any()).Return(mockVol)
		mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
				inode := parentIno + 1
				if name == "usr" {
					inode = 100
				}
				fileType := uint32(os.ModeDir)
				return &sdk.DirInfo{
					Name:  name,
					Inode: inode,
					Type:  fileType,
				}, nil
			}).Times(6)
		mockVol.EXPECT().GetInode(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, ino uint64) (*sdk.InodeInfo, error) {
				return &sdk.InodeInfo{
					Inode:      ino,
					Mode:       uint32(os.ModeDir),
					Nlink:      0,
					Size:       0,
					ModifyTime: time.Now(),
					CreateTime: time.Now(),
					AccessTime: time.Now(),
				}, nil
			}).Times(2)
		mockVol.EXPECT().CreateFile(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, error) {
				return &sdk.InodeInfo{
					Inode:      parentIno + 1,
					Mode:       uint32(os.ModeIrregular),
					Nlink:      0,
					Size:       0,
					ModifyTime: time.Now(),
					CreateTime: time.Now(),
					AccessTime: time.Now(),
				}, nil
			})
		mockVol.EXPECT().SetXAttr(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(sdk.ErrForbidden)
		tgt := fmt.Sprintf("%s/v1/drive", ts.URL)
		req, err := http.NewRequest(http.MethodPost, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, sdk.ErrForbidden.Status)
		res.Body.Close()
	}
}

func TestHandleAddUserConfig(t *testing.T) {
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
		clusters:    []string{"1", "2"},
	}
	ts := httptest.NewServer(d.RegisterAPIRouters())
	defer ts.Close()

	client := ts.Client()

	{
		urm.Set("test", &UserRoute{
			Uid:        UserID("test"),
			ClusterID:  "1",
			VolumeID:   "1",
			RootPath:   getRootPath("test"),
			RootFileID: 4,
		})
		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(mockCluster)
		mockCluster.EXPECT().GetVol(gomock.Any()).Return(mockVol)
		mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
				inode := parentIno + 1
				fileType := uint32(os.ModeDir)
				return &sdk.DirInfo{
					Name:  name,
					Inode: inode,
					Type:  fileType,
				}, nil
			})
		mockVol.EXPECT().GetInode(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, ino uint64) (*sdk.InodeInfo, error) {
				return &sdk.InodeInfo{
					Inode:      ino,
					Mode:       uint32(os.ModeDir),
					Nlink:      0,
					Size:       0,
					ModifyTime: time.Now(),
					CreateTime: time.Now(),
					AccessTime: time.Now(),
				}, nil
			})
		mockVol.EXPECT().CreateFile(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, error) {
				return &sdk.InodeInfo{
					Inode:      parentIno + 1,
					Mode:       uint32(os.ModeIrregular),
					Nlink:      0,
					Size:       0,
					ModifyTime: time.Now(),
					CreateTime: time.Now(),
					AccessTime: time.Now(),
				}, nil
			})
		mockVol.EXPECT().SetXAttr(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

		tgt := fmt.Sprintf("%s/v1/route?path=%s", ts.URL, url.QueryEscape("/test"))
		req, err := http.NewRequest(http.MethodPost, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, http.StatusOK)
		res.Body.Close()
		urm.Remove("test")
	}

	{
		tgt := fmt.Sprintf("%s/v1/route", ts.URL)
		req, err := http.NewRequest(http.MethodPost, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, http.StatusBadRequest)
		res.Body.Close()
	}

	{
		mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, sdk.ErrNotDir)
		tgt := fmt.Sprintf("%s/v1/route?path=%s", ts.URL, url.QueryEscape("/test"))
		req, err := http.NewRequest(http.MethodPost, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, sdk.ErrNotDir.Status)
		res.Body.Close()
	}
	urm.Set("test", &UserRoute{
		Uid:        UserID("test"),
		ClusterID:  "1",
		VolumeID:   "1",
		RootPath:   getRootPath("test"),
		RootFileID: 4,
	})
	defer urm.Remove("test")
	{
		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(nil)
		tgt := fmt.Sprintf("%s/v1/route?path=%s", ts.URL, url.QueryEscape("/test"))
		req, err := http.NewRequest(http.MethodPost, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, sdk.ErrNoCluster.Status)
		res.Body.Close()
	}

	{
		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(mockCluster)
		mockCluster.EXPECT().GetVol(gomock.Any()).Return(mockVol)
		mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, sdk.ErrExist)
		tgt := fmt.Sprintf("%s/v1/route?path=%s", ts.URL, url.QueryEscape("/test"))
		req, err := http.NewRequest(http.MethodPost, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, sdk.ErrExist.Status)
		res.Body.Close()
	}

	{
		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(mockCluster)
		mockCluster.EXPECT().GetVol(gomock.Any()).Return(mockVol)
		mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
				inode := parentIno + 1
				fileType := uint32(os.ModeDir)
				return &sdk.DirInfo{
					Name:  name,
					Inode: inode,
					Type:  fileType,
				}, nil
			})
		mockVol.EXPECT().GetInode(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, ino uint64) (*sdk.InodeInfo, error) {
				return &sdk.InodeInfo{
					Inode:      ino,
					Mode:       uint32(os.ModeDir),
					Nlink:      0,
					Size:       0,
					ModifyTime: time.Now(),
					CreateTime: time.Now(),
					AccessTime: time.Now(),
				}, nil
			})
		mockVol.EXPECT().CreateFile(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, sdk.ErrConflict)
		tgt := fmt.Sprintf("%s/v1/route?path=%s", ts.URL, url.QueryEscape("/test"))
		req, err := http.NewRequest(http.MethodPost, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, sdk.ErrConflict.Status)
		res.Body.Close()
	}
}
