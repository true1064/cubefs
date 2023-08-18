package drive

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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
