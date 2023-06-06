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
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestHandleSetProperties(t *testing.T) {
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

	client := ts.Client()
	{
		tgt := fmt.Sprintf("%s/v1/files/properties?path=%s", ts.URL, url.QueryEscape("/test"))
		req, err := http.NewRequest(http.MethodPut, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		res.Body.Close()
		require.Equal(t, res.StatusCode, http.StatusOK)
	}

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
				if name == "test" {
					return &sdk.DirInfo{
						Name:  name,
						Inode: parentIno + 1,
						Type:  uint32(os.ModeDir),
					}, nil
				}
				return nil, sdk.ErrNotFound
			})
		mockVol.EXPECT().BatchSetXAttr(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

		tgt := fmt.Sprintf("%s/v1/files/properties?path=%s", ts.URL, url.QueryEscape("/test"))
		req, err := http.NewRequest(http.MethodPut, tgt, nil)
		req.Header.Set(headerUserID, "test")
		req.Header.Set("x-cfa-meta-mykey", "12345")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		res.Body.Close()
		require.Equal(t, res.StatusCode, http.StatusOK)
		urm.Remove("test")
	}

	{
		tgt := fmt.Sprintf("%s/v1/files/properties", ts.URL)
		req, err := http.NewRequest(http.MethodPut, tgt, nil)
		req.Header.Set(headerUserID, "test")
		req.Header.Set("x-cfa-meta-mykey", "12345")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		res.Body.Close()
		require.Equal(t, res.StatusCode, http.StatusBadRequest)
	}

	{
		urm.Set("test", &UserRoute{
			Uid:        UserID("test"),
			ClusterID:  "1",
			VolumeID:   "1",
			RootPath:   getRootPath("test"),
			RootFileID: 4,
		})

		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(nil)
		tgt := fmt.Sprintf("%s/v1/files/properties?path=%s", ts.URL, url.QueryEscape("/test"))
		req, err := http.NewRequest(http.MethodPut, tgt, nil)
		req.Header.Set(headerUserID, "test")
		req.Header.Set("x-cfa-meta-mykey", "12345")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		res.Body.Close()
		require.Equal(t, res.StatusCode, sdk.ErrNoCluster.Status)
		urm.Remove("test")
	}

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
		mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, sdk.ErrNotFound)

		tgt := fmt.Sprintf("%s/v1/files/properties?path=%s", ts.URL, url.QueryEscape("/test"))
		req, err := http.NewRequest(http.MethodPut, tgt, nil)
		req.Header.Set(headerUserID, "test")
		req.Header.Set("x-cfa-meta-mykey", "12345")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		res.Body.Close()
		require.Equal(t, res.StatusCode, sdk.ErrNotFound.Status)
		urm.Remove("test")
	}

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
				if name == "test" {
					return &sdk.DirInfo{
						Name:  name,
						Inode: parentIno + 1,
						Type:  uint32(os.ModeDir),
					}, nil
				}
				return nil, sdk.ErrNotFound
			})
		mockVol.EXPECT().BatchSetXAttr(gomock.Any(), gomock.Any(), gomock.Any()).Return(sdk.ErrConflict)
		tgt := fmt.Sprintf("%s/v1/files/properties?path=%s", ts.URL, url.QueryEscape("/test"))
		req, err := http.NewRequest(http.MethodPut, tgt, nil)
		req.Header.Set(headerUserID, "test")
		req.Header.Set("x-cfa-meta-mykey", "12345")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		res.Body.Close()
		require.Equal(t, res.StatusCode, sdk.ErrConflict.Status)
		urm.Remove("test")
	}
}

func TestHandleDelProperties(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(path string, keys ...string) rpc.HTTPError {
		url := genURL(server.URL, "/v1/files/properties", "path", path)
		req, _ := http.NewRequest(http.MethodDelete, url, nil)
		req.Header.Add(headerUserID, testUserID)
		for _, k := range keys {
			v := ""
			if len(k)%2 == 0 {
				v = "1"
			}
			req.Header.Add(userPropertyPrefix+k, v)
		}
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		if err = rpc.ParseData(resp, nil); err != nil {
			return err.(rpc.HTTPError)
		}
		return nil
	}

	{
		require.Equal(t, 400, doRequest("").StatusCode())
		require.Equal(t, 400, doRequest("a/b/../../..").StatusCode())
	}
	{
		require.NoError(t, doRequest("a"))
		require.NoError(t, doRequest("a"))
		require.NoError(t, doRequest("a", "", ""))
	}
	{
		node.OnceGetUser(testUserID)
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		require.Equal(t, e1.Status, doRequest("a", "k1").StatusCode())
	}
	{
		node.OnceGetUser()
		node.OnceLookup(false)
		node.Volume.EXPECT().BatchDeleteXAttr(A, A, A).Return(e2)
		require.Equal(t, e2.Status, doRequest("a", "k1", "k10").StatusCode())
	}
	{
		node.OnceGetUser()
		node.OnceLookup(false)
		node.Volume.EXPECT().BatchDeleteXAttr(A, A, A).Return(nil)
		require.NoError(t, doRequest("a", "k1", "k10", "k-100"))
	}
}

func TestHandleGetProperties(t *testing.T) {
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
				if name == "test" {
					return &sdk.DirInfo{
						Name:  name,
						Inode: parentIno + 1,
						Type:  uint32(os.ModeDir),
					}, nil
				}
				return nil, sdk.ErrNotFound
			})
		mockVol.EXPECT().GetXAttrMap(gomock.Any(), gomock.Any()).Return(map[string]string{"mytest": "1234567"}, nil)
		mockVol.EXPECT().GetInode(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, ino uint64) (*sdk.InodeInfo, error) {
				return &sdk.InodeInfo{
					Inode:      ino,
					Mode:       uint32(os.ModeIrregular),
					Size:       1024,
					ModifyTime: time.Now(),
					CreateTime: time.Now(),
					AccessTime: time.Now(),
				}, nil
			})

		tgt := fmt.Sprintf("%s/v1/files/properties?path=%s", ts.URL, url.QueryEscape("/test"))
		req, err := http.NewRequest(http.MethodGet, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		require.Nil(t, err)
		body, err := io.ReadAll(res.Body)
		res.Body.Close()
		require.Nil(t, err)
		var result GetPropertiesResult
		json.Unmarshal(body, &result)
		require.Equal(t, res.StatusCode, http.StatusOK)
		require.Equal(t, result.Properties["mytest"], "1234567")
		urm.Remove("test")
	}

	{
		tgt := fmt.Sprintf("%s/v1/files/properties", ts.URL)
		req, err := http.NewRequest(http.MethodGet, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		res.Body.Close()
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, http.StatusBadRequest)
	}

	{
		urm.Set("test", &UserRoute{
			Uid:        UserID("test"),
			ClusterID:  "1",
			VolumeID:   "1",
			RootPath:   getRootPath("test"),
			RootFileID: 4,
		})

		mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(nil)
		tgt := fmt.Sprintf("%s/v1/files/properties?path=%s", ts.URL, url.QueryEscape("/test"))
		req, err := http.NewRequest(http.MethodGet, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		res.Body.Close()
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, sdk.ErrNoCluster.Status)
		urm.Remove("test")
	}

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
				return nil, sdk.ErrNotFound
			})

		tgt := fmt.Sprintf("%s/v1/files/properties?path=%s", ts.URL, url.QueryEscape("/test"))
		req, err := http.NewRequest(http.MethodGet, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		res.Body.Close()
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, sdk.ErrNotFound.Status)
		urm.Remove("test")
	}

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
				if name == "test" {
					return &sdk.DirInfo{
						Name:  name,
						Inode: parentIno + 1,
						Type:  uint32(os.ModeDir),
					}, nil
				}
				return nil, sdk.ErrNotFound
			})
		mockVol.EXPECT().GetXAttrMap(gomock.Any(), gomock.Any()).Return(nil, sdk.ErrForbidden)

		tgt := fmt.Sprintf("%s/v1/files/properties?path=%s", ts.URL, url.QueryEscape("/test"))
		req, err := http.NewRequest(http.MethodGet, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		res.Body.Close()
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, sdk.ErrForbidden.Status)
		urm.Remove("test")
	}

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
				if name == "test" {
					return &sdk.DirInfo{
						Name:  name,
						Inode: parentIno + 1,
						Type:  uint32(os.ModeDir),
					}, nil
				}
				return nil, sdk.ErrNotFound
			})
		mockVol.EXPECT().GetXAttrMap(gomock.Any(), gomock.Any()).Return(map[string]string{"mytest": "1234567"}, nil)
		mockVol.EXPECT().GetInode(gomock.Any(), gomock.Any()).Return(nil, sdk.ErrExist)

		tgt := fmt.Sprintf("%s/v1/files/properties?path=%s", ts.URL, url.QueryEscape("/test"))
		req, err := http.NewRequest(http.MethodGet, tgt, nil)
		req.Header.Set(headerUserID, "test")
		require.Nil(t, err)
		res, err := client.Do(req)
		res.Body.Close()
		require.Nil(t, err)
		require.Equal(t, res.StatusCode, sdk.ErrExist.Status)
		urm.Remove("test")
	}
}
