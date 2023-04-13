package drive

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/apinode/testing/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/singleflight"
)

func TestGetUserRouteInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	urm, _ := NewUserRouteMgr()
	mockVol := mocks.NewMockIVolume(ctrl)
	mockClusterMgr := mocks.NewMockClusterManager(ctrl)
	d := &DriveNode{
		vol:         mockVol,
		userRouter:  urm,
		clusterMgr:  mockClusterMgr,
		groupRouter: &singleflight.Group{},
	}

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("look up error"))
	_, err := d.GetUserRouteInfo(context.TODO(), "test")
	require.Equal(t, err.Error(), "look up error")

	_, h2 := hash("test")
	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, ino uint64, name string) (*sdk.DirInfo, error) {
			if name == fmt.Sprintf("%d", h2%hashMask) {
				return nil, sdk.ErrNotFound
			}
			return &sdk.DirInfo{
				Name:  name,
				Inode: uint64(rand.Int63()),
			}, nil
		}).AnyTimes()
	_, err = d.GetUserRouteInfo(context.TODO(), "test")
	require.ErrorIs(t, err, sdk.ErrNotFound)

	// not found xattr
	mockVol.EXPECT().GetXAttr(gomock.Any(), gomock.Any(), gomock.Any()).Return("", sdk.ErrNotFound)
	_, err = d.GetUserRouteInfo(context.TODO(), "test1")
	require.ErrorIs(t, err, sdk.ErrNotFound)

	ur := UserRoute{
		Uid:         "test1",
		ClusterType: 1,
		ClusterID:   "cluster01",
		VolumeID:    "volume01",
		DriveID:     "test1_drive",
		RootPath:    getRootPath("test1"),
		RootFileID:  10,
		Ctime:       time.Now().Unix(),
	}
	v, _ := json.Marshal(ur)
	mockVol.EXPECT().GetXAttr(gomock.Any(), gomock.Any(), gomock.Any()).Return(string(v), nil)
	ur1, err := d.GetUserRouteInfo(context.TODO(), "test1")
	require.Equal(t, *ur1, ur)
}

func TestGetRootInoAndVolume(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	urm, _ := NewUserRouteMgr()
	mockVol := mocks.NewMockIVolume(ctrl)
	mockClusterMgr := mocks.NewMockClusterManager(ctrl)
	d := &DriveNode{
		vol:         mockVol,
		userRouter:  urm,
		clusterMgr:  mockClusterMgr,
		groupRouter: &singleflight.Group{},
	}

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, ino uint64, name string) (*sdk.DirInfo, error) {
			return &sdk.DirInfo{
				Name:  name,
				Inode: uint64(rand.Int63()),
			}, nil
		}).AnyTimes()
	mockVol.EXPECT().GetXAttr(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, ino uint64, key string) (string, error) {
		ur := UserRoute{
			Uid:         "test1",
			ClusterType: 1,
			ClusterID:   "cluster01",
			VolumeID:    "volume01",
			DriveID:     "test1_drive",
			RootPath:    getRootPath("test1"),
			RootFileID:  10,
			Ctime:       time.Now().Unix(),
		}
		v, _ := json.Marshal(ur)
		return string(v), nil
	}).AnyTimes()
	mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(nil)
	_, _, err := d.getRootInoAndVolume(context.TODO(), "test1")
	require.ErrorIs(t, err, sdk.ErrNoCluster)

	mockCluster := mocks.NewMockICluster(ctrl)
	mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(mockCluster).AnyTimes()
	mockCluster.EXPECT().GetVol(gomock.Any()).Return(nil)

	_, _, err = d.getRootInoAndVolume(context.TODO(), "test1")
	require.ErrorIs(t, err, sdk.ErrNoVolume)

	mockVol1 := mocks.NewMockIVolume(ctrl)
	mockCluster.EXPECT().GetVol(gomock.Any()).Return(mockVol1)
	rootIno, vol, err := d.getRootInoAndVolume(context.TODO(), "test1")
	require.Nil(t, err)
	require.Equal(t, rootIno, Inode(10))
	require.Equal(t, vol, mockVol1)
}

func TestLookup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	d := &DriveNode{}
	mockVol := mocks.NewMockIVolume(ctrl)

	_, err := d.lookup(context.TODO(), mockVol, 1, "/")
	require.ErrorIs(t, err, sdk.ErrBadRequest)

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, sdk.ErrNotFound)
	_, err = d.lookup(context.TODO(), mockVol, 1, "/a/")
	require.ErrorIs(t, err, sdk.ErrNotFound)

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
		return &sdk.DirInfo{
			Name:  name,
			Inode: parentIno + 1,
		}, nil
	}).Times(3)
	dirInfo, err := d.lookup(context.TODO(), mockVol, 1, "/a/b/c")
	require.ErrorIs(t, err, nil)
	require.Equal(t, dirInfo.Name, "c")
	require.Equal(t, dirInfo.Inode, uint64(4))
}

func TestCreateDir(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockVol := mocks.NewMockIVolume(ctrl)
	d := &DriveNode{}
	_, err := d.createDir(context.TODO(), mockVol, 1, "/", false)
	require.ErrorIs(t, err, sdk.ErrBadRequest)

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, sdk.ErrNotFound).Times(2)
	_, err = d.createDir(context.TODO(), mockVol, 1, "/a/b", false)
	require.ErrorIs(t, err, sdk.ErrNotFound)
	mockVol.EXPECT().Mkdir(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, sdk.ErrForbidden)
	_, err = d.createDir(context.TODO(), mockVol, 1, "/a", false)
	require.ErrorIs(t, err, sdk.ErrForbidden)

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
			return &sdk.DirInfo{
				Name:  name,
				Inode: parentIno + 1,
				Type:  uint32(os.ModeIrregular),
			}, nil
		})
	_, err = d.createDir(context.TODO(), mockVol, 1, "/a/b", false)
	require.ErrorIs(t, err, sdk.ErrConflict)

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
			if name == "c" {
				return nil, sdk.ErrNotFound
			}
			return &sdk.DirInfo{
				Name:  name,
				Inode: parentIno + 1,
				Type:  uint32(os.ModeDir),
			}, nil
		}).Times(3)
	mockVol.EXPECT().Mkdir(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, error) {
			return &sdk.InodeInfo{
				Inode:      parentIno + 1,
				Mode:       uint32(os.ModeDir),
				Nlink:      1,
				Size:       4096,
				Uid:        0,
				Gid:        0,
				ModifyTime: time.Now(),
				CreateTime: time.Now(),
				AccessTime: time.Now(),
			}, nil
		})
	inoInfo, err := d.createDir(context.TODO(), mockVol, 1, "/a/b/c", false)
	require.Nil(t, err)
	require.Equal(t, inoInfo.Inode, uint64(4))
	require.True(t, os.FileMode(inoInfo.Mode).IsDir())

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, sdk.ErrNotFound).Times(3)
	mockVol.EXPECT().Mkdir(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, error) {
			return &sdk.InodeInfo{
				Inode:      parentIno + 1,
				Mode:       uint32(os.ModeDir),
				Nlink:      1,
				Size:       4096,
				Uid:        0,
				Gid:        0,
				ModifyTime: time.Now(),
				CreateTime: time.Now(),
				AccessTime: time.Now(),
			}, nil
		}).Times(3)
	inoInfo, err = d.createDir(context.TODO(), mockVol, 1, "/a/b/c", true)
	require.Nil(t, err)
	require.Equal(t, inoInfo.Inode, uint64(4))
	require.True(t, os.FileMode(inoInfo.Mode).IsDir())

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
			return &sdk.DirInfo{
				Name:  name,
				Inode: parentIno + 1,
				Type:  uint32(os.ModeDir),
			}, nil
		}).Times(3)
	mockVol.EXPECT().GetInode(gomock.Any(), gomock.Any()).Return(nil, sdk.ErrNotFound)
	_, err = d.createDir(context.TODO(), mockVol, 1, "/a/b/c", true)
	require.ErrorIs(t, err, sdk.ErrNotFound)

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
			return &sdk.DirInfo{
				Name:  name,
				Inode: parentIno + 1,
				Type:  uint32(os.ModeDir),
			}, nil
		}).Times(3)
	mockVol.EXPECT().GetInode(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, inode uint64) (*sdk.InodeInfo, error) {
			return &sdk.InodeInfo{
				Inode: inode,
				Mode:  uint32(os.ModeDir),
			}, nil
		})
	inoInfo, err = d.createDir(context.TODO(), mockVol, 1, "/a/b/c", true)
	require.Nil(t, err)
	require.Equal(t, inoInfo.Inode, uint64(4))
	require.True(t, os.FileMode(inoInfo.Mode).IsDir())
}

func TestCreateFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockVol := mocks.NewMockIVolume(ctrl)
	d := &DriveNode{}

	_, err := d.createFile(context.TODO(), mockVol, 1, "/")
	require.ErrorIs(t, err, sdk.ErrBadRequest)

	mockVol.EXPECT().CreateFile(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, sdk.ErrBadFile)
	_, err = d.createFile(context.Background(), mockVol, 1, "a")
	require.ErrorIs(t, err, sdk.ErrBadFile)

	mockVol.EXPECT().CreateFile(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, error) {
			return &sdk.InodeInfo{
				Inode: parentIno + 1,
				Mode:  uint32(os.ModeIrregular),
				Size:  0,
			}, nil
		})
	inoInfo, err := d.createFile(context.Background(), mockVol, 1, "a")
	require.Nil(t, err)
	require.Equal(t, inoInfo.Inode, uint64(2))

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
			return &sdk.DirInfo{
				Name:  name,
				Inode: parentIno + 1,
				Type:  uint32(os.ModeDir),
			}, nil
		}).Times(2)
	mockVol.EXPECT().GetInode(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, inode uint64) (*sdk.InodeInfo, error) {
			return &sdk.InodeInfo{
				Inode: inode,
				Mode:  uint32(os.ModeDir),
			}, nil
		})
	mockVol.EXPECT().CreateFile(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, error) {
			return &sdk.InodeInfo{
				Inode: parentIno + 1,
				Mode:  uint32(os.ModeIrregular),
				Size:  0,
			}, nil
		})
	inoInfo, err = d.createFile(context.TODO(), mockVol, 1, "/a/b/c")
	require.Nil(t, err)
	require.Equal(t, inoInfo.Inode, uint64(4))

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
			mode := uint32(os.ModeDir)
			if name == "c" {
				mode = uint32(os.ModeIrregular)
			}
			return &sdk.DirInfo{
				Name:  name,
				Inode: parentIno + 1,
				Type:  mode,
			}, nil
		}).Times(3)
	mockVol.EXPECT().GetInode(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, inode uint64) (*sdk.InodeInfo, error) {
			mode := uint32(os.ModeDir)
			if inode == 4 {
				mode = uint32(os.ModeIrregular)
			}
			return &sdk.InodeInfo{
				Inode: inode,
				Mode:  mode,
			}, nil
		}).Times(2)

	mockVol.EXPECT().CreateFile(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, error) {
			return nil, sdk.ErrExist
		})
	inoInfo, err = d.createFile(context.TODO(), mockVol, 1, "/a/b/c")
	require.Nil(t, err)
	require.Equal(t, inoInfo.Inode, uint64(4))
}
