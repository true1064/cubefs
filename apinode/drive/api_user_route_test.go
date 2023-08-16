package drive

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
)

func TestHandleCreateDrive(t *testing.T) {
	ctrl := gomock.NewController(t)
	urm, _ := NewUserRouteMgr()
	mockCluster := mocks.NewMockICluster(ctrl)
	mockVol := mocks.NewMockIVolume(ctrl)
	mockClusterMgr := mocks.NewMockClusterManager(ctrl)
	d := &DriveNode{
		vol:        mockVol,
		userRouter: urm,
		clusterMgr: mockClusterMgr,
		cryptor:    newMockCryptor(t),
		clusters:   []string{"1", "2"},
	}
	ts := httptest.NewServer(d.RegisterAPIRouters())
	defer ts.Close()

	client := ts.Client()

	vols := []*sdk.VolInfo{
		{Name: "1", Weight: 10},
		{Name: "2", Weight: 10},
	}
	{
		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(mockCluster)
		mockCluster.EXPECT().ListVols().Return(vols)
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
		mockVol.EXPECT().CreateFile(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, uint64, error) {
				return &sdk.InodeInfo{
					Inode:      parentIno + 1,
					Mode:       uint32(os.ModeIrregular),
					Nlink:      0,
					Size:       0,
					ModifyTime: time.Now(),
					CreateTime: time.Now(),
					AccessTime: time.Now(),
				}, 0, nil
			})
		mockVol.EXPECT().SetXAttr(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

		tgt := fmt.Sprintf("%s/v1/drive", ts.URL)
		req, err := http.NewRequest(http.MethodPut, tgt, nil)
		req.Header.Set(HeaderUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, http.StatusOK)
		res.Body.Close()
	}

	{
		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(nil)
		tgt := fmt.Sprintf("%s/v1/drive", ts.URL)
		req, err := http.NewRequest(http.MethodPut, tgt, nil)
		req.Header.Set(HeaderUserID, "test")
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
		req, err := http.NewRequest(http.MethodPut, tgt, nil)
		req.Header.Set(HeaderUserID, "test")
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
		req, err := http.NewRequest(http.MethodPut, tgt, nil)
		req.Header.Set(HeaderUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, sdk.ErrNoVolume.Status)
		res.Body.Close()
	}

	{
		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(mockCluster)
		mockCluster.EXPECT().ListVols().Return(vols)
		mockCluster.EXPECT().GetVol(gomock.Any()).Return(nil)
		tgt := fmt.Sprintf("%s/v1/drive", ts.URL)
		req, err := http.NewRequest(http.MethodPut, tgt, nil)
		req.Header.Set(HeaderUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, sdk.ErrNoVolume.Status)
		res.Body.Close()
	}

	{
		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(mockCluster)
		mockCluster.EXPECT().ListVols().Return(vols)
		mockCluster.EXPECT().GetVol(gomock.Any()).Return(mockVol)
		mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, sdk.ErrConflict)
		tgt := fmt.Sprintf("%s/v1/drive", ts.URL)
		req, err := http.NewRequest(http.MethodPut, tgt, nil)
		req.Header.Set(HeaderUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, sdk.ErrConflict.Status)
		res.Body.Close()
	}

	{
		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(mockCluster)
		mockCluster.EXPECT().ListVols().Return(vols)
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
		mockVol.EXPECT().CreateFile(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, uint64, error) {
				return &sdk.InodeInfo{
					Inode:      parentIno + 1,
					Mode:       uint32(os.ModeIrregular),
					Nlink:      0,
					Size:       0,
					ModifyTime: time.Now(),
					CreateTime: time.Now(),
					AccessTime: time.Now(),
				}, 0, nil
			})
		mockVol.EXPECT().SetXAttr(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(sdk.ErrForbidden)
		tgt := fmt.Sprintf("%s/v1/drive", ts.URL)
		req, err := http.NewRequest(http.MethodPut, tgt, nil)
		req.Header.Set(HeaderUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, sdk.ErrForbidden.Status)
		res.Body.Close()
	}
}

func TestHandleGetDrive(t *testing.T) {
	ctrl := gomock.NewController(t)
	urm, _ := NewUserRouteMgr()
	mockVol := mocks.NewMockIVolume(ctrl)
	mockClusterMgr := mocks.NewMockClusterManager(ctrl)
	d := &DriveNode{
		vol:        mockVol,
		userRouter: urm,
		clusterMgr: mockClusterMgr,
		cryptor:    newMockCryptor(t),
		clusters:   []string{"1", "2"},
	}
	ts := httptest.NewServer(d.RegisterAPIRouters())
	defer ts.Close()

	client := ts.Client()

	{
		tgt := fmt.Sprintf("%s/v1/drive?uid=test", ts.URL)
		req, err := http.NewRequest(http.MethodGet, tgt, nil)
		req.Header.Set(HeaderUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, http.StatusForbidden)
		res.Body.Close()
	}

	{
		d.admin = "test"
		ur := &UserRoute{
			Uid:        UserID("test1"),
			ClusterID:  "1",
			VolumeID:   "1",
			RootPath:   getRootPath("/test/myroot"),
			RootFileID: 4,
		}
		urm.Set("test1", ur)

		tgt := fmt.Sprintf("%s/v1/drive?uid=test1", ts.URL)
		req, err := http.NewRequest(http.MethodGet, tgt, nil)
		req.Header.Set(HeaderUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, http.StatusOK)
		data, err := io.ReadAll(res.Body)
		require.Nil(t, err)
		res.Body.Close()
		r := &UserRoute{}
		json.Unmarshal(data, r)
		require.Equal(t, *r, *ur)
	}
}

func TestHandleUpdateDrive(t *testing.T) {
	ctrl := gomock.NewController(t)
	urm, _ := NewUserRouteMgr()
	mockVol := mocks.NewMockIVolume(ctrl)
	mockClusterMgr := mocks.NewMockClusterManager(ctrl)
	d := &DriveNode{
		vol:        mockVol,
		userRouter: urm,
		clusterMgr: mockClusterMgr,
		cryptor:    newMockCryptor(t),
		clusters:   []string{"1", "2"},
	}
	ts := httptest.NewServer(d.RegisterAPIRouters())
	defer ts.Close()

	client := ts.Client()

	{
		tgt := fmt.Sprintf("%s/v1/drive?uid=test", ts.URL)
		req, err := http.NewRequest(http.MethodPatch, tgt, nil)
		req.Header.Set(HeaderUserID, "admin")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, http.StatusForbidden)
		res.Body.Close()
	}

	{
		tgt := fmt.Sprintf("%s/v1/drive?uid=test&rootPath=%s", ts.URL, url.QueryEscape("/test/myroot"))
		req, err := http.NewRequest(http.MethodPatch, tgt, nil)
		req.Header.Set(HeaderUserID, "admin")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, http.StatusForbidden)
		res.Body.Close()
	}

	{
		d.admin = "admin"
		mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, sdk.ErrNotFound)
		tgt := fmt.Sprintf("%s/v1/drive?uid=test&rootPath=%s", ts.URL, url.QueryEscape("/test/myroot"))
		req, err := http.NewRequest(http.MethodPatch, tgt, nil)
		req.Header.Set(HeaderUserID, "admin")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, sdk.ErrNoUser.Status)
		res.Body.Close()
	}

	{
		d.admin = "admin"
		urm.Set("test", &UserRoute{
			Uid:        UserID("test"),
			ClusterID:  "1",
			VolumeID:   "1",
			RootPath:   getRootPath("/test/myroot"),
			RootFileID: 4,
		})
		ur := d.userRouter.Get("test")
		require.Equal(t, ur.RootFileID, FileID(4))
		mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
				inode := parentIno + 1
				fileType := uint32(os.ModeDir)
				return &sdk.DirInfo{
					Name:  name,
					Inode: inode,
					Type:  fileType,
				}, nil
			}).Times(4)
		mockVol.EXPECT().SetXAttr(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
		tgt := fmt.Sprintf("%s/v1/drive?uid=test&rootPath=%s", ts.URL, url.QueryEscape("/test/myroot2"))
		req, err := http.NewRequest(http.MethodPatch, tgt, nil)
		req.Header.Set(HeaderUserID, "admin")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, http.StatusOK)
		ur = urm.Get("test")
		require.Equal(t, ur.RootPath, "/test/myroot2")
		res.Body.Close()
		d.userRouter.Remove("test")
	}
}
