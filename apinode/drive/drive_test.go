package drive

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/apinode/oplog"
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/apinode/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/cubefs/cubefs/proto"
)

var (
	A = gomock.Any()
	C = gomock.NewController

	Ctx = context.Background()

	testUserID  = UserID{ID: "test-user-1"}
	testUserAPP = UserID{ID: "app", Public: true}

	e1, e2, e3, e4 = randError(1), randError(2), randError(3), randError(4)
)

func randError(num int) *sdk.Error {
	rand.Seed(time.Now().UnixNano())
	st := rand.Intn(80) + 520
	return &sdk.Error{
		Status:  st,
		Code:    fmt.Sprintf("[n:%d code:%d]", num, st),
		Message: fmt.Sprintf("[n:%d message:%d]", num, st),
	}
}

type mockNode struct {
	DriveNode  *DriveNode
	Volume     *mocks.MockIVolume
	ClusterMgr *mocks.MockClusterManager
	Cluster    *mocks.MockICluster
	GenInode   func() uint64

	cachedUser map[UserID]struct{}
}

func newMockCryptor(tb testing.TB) *mocks.MockCryptor {
	transmitter := mocks.NewMockTransmitter(C(tb))
	transmitter.EXPECT().Encrypt(A, A).DoAndReturn(func(text string, _ bool) (string, error) { return text, nil }).AnyTimes()
	transmitter.EXPECT().Decrypt(A, A).DoAndReturn(func(text string, _ bool) (string, error) { return text, nil }).AnyTimes()

	cryptor := mocks.NewMockCryptor(C(tb))
	cryptor.EXPECT().TransEncryptor(A, A).DoAndReturn(func(_ string, r io.Reader) (io.Reader, string, error) { return r, "", nil }).AnyTimes()
	cryptor.EXPECT().TransDecryptor(A, A).DoAndReturn(func(_ string, r io.Reader) (io.Reader, error) { return r, nil }).AnyTimes()
	cryptor.EXPECT().GenKey().Return(nil, nil).AnyTimes()
	cryptor.EXPECT().FileEncryptor(A, A).DoAndReturn(func(_ []byte, r io.Reader) (io.Reader, error) { return r, nil }).AnyTimes()
	cryptor.EXPECT().FileDecryptor(A, A).DoAndReturn(func(_ []byte, r io.Reader) (io.Reader, error) { return r, nil }).AnyTimes()
	cryptor.EXPECT().Transmitter(A).Return(transmitter, nil).AnyTimes()
	return cryptor
}

func newMockNode(tb testing.TB) mockNode {
	urm, _ := NewUserRouteMgr()
	volume := mocks.NewMockIVolume(C(tb))
	clusterMgr := mocks.NewMockClusterManager(C(tb))
	inode := uint64(1)
	return mockNode{
		DriveNode: &DriveNode{
			vol:        volume,
			userRouter: urm,
			clusterMgr: clusterMgr,
			cryptor:    newMockCryptor(tb),
			out:        oplog.NewOutput(),
		},
		Volume:     volume,
		ClusterMgr: clusterMgr,
		Cluster:    mocks.NewMockICluster(C(tb)),
		GenInode: func() uint64 {
			return atomic.AddUint64(&inode, 1)
		},
		cachedUser: make(map[UserID]struct{}),
	}
}

func (node *mockNode) AddUserRoute(uids ...UserID) {
	for _, uid := range uids {
		if _, ok := node.cachedUser[uid]; ok {
			continue
		}
		node.cachedUser[uid] = struct{}{}

		path := getUserRouteFile(uid)
		LookupN := len(strings.Split(strings.Trim(path, "/"), "/"))
		node.Volume.EXPECT().Lookup(A, A, A).DoAndReturn(
			func(_ context.Context, _ uint64, name string) (*sdk.DirInfo, error) {
				return &sdk.DirInfo{
					Name:  name,
					Inode: node.GenInode(),
				}, nil
			}).Times(LookupN)
		node.Volume.EXPECT().GetXAttr(A, A, A).DoAndReturn(
			func(ctx context.Context, ino uint64, key string) (string, error) {
				uid := UserID{ID: key}
				ur := UserRoute{
					Uid:         uid.ID,
					ClusterType: 1,
					ClusterID:   "cluster01",
					VolumeID:    "volume01",
					DriveID:     uid.String() + "_drive",
					RootPath:    getRootPath(uid),
					RootFileID:  FileID(ino),
					Ctime:       time.Now().Unix(),
				}
				val, _ := ur.Marshal()
				return string(val), nil
			})
	}
}

func (node *mockNode) OnceGetUser(uids ...UserID) {
	node.AddUserRoute(uids...)
	node.ClusterMgr.EXPECT().GetCluster(A).Return(node.Cluster)
	node.Cluster.EXPECT().GetVol(A).Return(node.Volume)
	node.Volume.EXPECT().GetDirSnapshot(A, A).Return(node.Volume, nil)
}

func (node *mockNode) GetUserN(n int, uids ...UserID) {
	node.AddUserRoute(uids...)
	node.ClusterMgr.EXPECT().GetCluster(A).Return(node.Cluster).Times(n)
	node.Cluster.EXPECT().GetVol(A).Return(node.Volume).Times(n)
	node.Volume.EXPECT().GetDirSnapshot(A, A).Return(node.Volume, nil).Times(n)
}

func (node *mockNode) GetUserN2(uids ...UserID) {
	node.GetUserN(2, uids...)
}

func (node *mockNode) GetUserAny(uids ...UserID) {
	node.AddUserRoute(uids...)
	node.ClusterMgr.EXPECT().GetCluster(A).Return(node.Cluster).AnyTimes()
	node.Cluster.EXPECT().GetVol(A).Return(node.Volume).AnyTimes()
	node.Volume.EXPECT().GetDirSnapshot(A, A).Return(node.Volume, nil).AnyTimes()
}

func (node *mockNode) TestGetUser(tb testing.TB, request func() rpc.HTTPError, uids ...UserID) {
	node.AddUserRoute(uids...)
	node.ClusterMgr.EXPECT().GetCluster(A).Return(nil)
	require.Equal(tb, sdk.ErrNoCluster.Status, request().StatusCode())
}

func (node *mockNode) OnceLookup(isDir bool) {
	typ := uint32(0)
	if isDir {
		typ = uint32(fs.ModeDir)
	}
	node.Volume.EXPECT().Lookup(A, A, A).DoAndReturn(
		func(_ context.Context, _ uint64, name string) (*sdk.DirInfo, error) {
			return &sdk.DirInfo{
				Name:  name,
				Inode: node.GenInode(),
				Type:  typ,
			}, nil
		})
}

func (node *mockNode) AnyLookup() {
	node.Volume.EXPECT().Lookup(A, A, A).DoAndReturn(
		func(_ context.Context, _ uint64, name string) (*sdk.DirInfo, error) {
			return &sdk.DirInfo{
				Name:  name,
				Inode: node.GenInode(),
			}, nil
		},
	).AnyTimes()
}

func (node *mockNode) LookupN(n int) {
	for i := 0; i < n; i++ {
		node.OnceLookup(false)
	}
}

func (node *mockNode) LookupDirN(n int) {
	for i := 0; i < n; i++ {
		node.OnceLookup(true)
	}
}

func (node *mockNode) OnceGetInode() {
	node.Volume.EXPECT().GetInode(A, A).DoAndReturn(
		func(_ context.Context, ino uint64) (*proto.InodeInfo, error) {
			return &proto.InodeInfo{
				Size:  1024,
				Inode: ino,
			}, nil
		})
}

func (node *mockNode) ListDir(n, nFile int) {
	node.Volume.EXPECT().Readdir(A, A, A, A).DoAndReturn(
		func(context.Context, uint64, string, uint32) ([]sdk.DirInfo, error) {
			dirs := make([]sdk.DirInfo, n)
			for i := range dirs {
				dirs[i].Inode = node.GenInode()
				dirs[i].Name = fmt.Sprintf("inode_%d", dirs[i].Inode)
				dirs[i].Type = uint32(fs.ModeDir)
			}
			for i := 0; i < nFile; i++ {
				dirs[i].Type = 0
			}
			return dirs, nil
		})
	node.Volume.EXPECT().BatchGetInodes(A, A).DoAndReturn(
		func(_ context.Context, inos []uint64) ([]*proto.InodeInfo, error) {
			r := make([]*proto.InodeInfo, len(inos))
			for i := range r {
				r[i] = &proto.InodeInfo{Inode: inos[i]}
			}
			return r, nil
		})
	node.Volume.EXPECT().GetXAttrMap(A, A).Return(nil, nil).Times(n)
}

func resp2Data(resp *http.Response, data interface{}) rpc.HTTPError {
	if err := rpc.ParseData(resp, data); err != nil {
		return err.(rpc.HTTPError)
	}
	return nil
}

func resp2Error(resp *http.Response) rpc.HTTPError {
	return resp2Data(resp, nil)
}

// httptest server need close by you.
func newTestServer(d *DriveNode) (*httptest.Server, rpc.Client) {
	server := httptest.NewServer(d.RegisterAPIRouters())
	return server, rpc.NewClient(&rpc.Config{})
}

func genURL(host string, uri string, queries ...string) string {
	if len(queries)%2 == 1 {
		queries = append(queries, "")
	}
	q := make(url.Values)
	for i := 0; i < len(queries); i += 2 {
		q.Set(queries[i], queries[i+1])
	}
	if len(q) > 0 {
		return fmt.Sprintf("%s%s?%s", host, uri, q.Encode())
	}
	return fmt.Sprintf("%s%s", host, uri)
}

type mockBody struct {
	buff []byte
}

func (r *mockBody) Read(p []byte) (n int, err error) {
	if r == nil || len(r.buff) == 0 {
		return 0, io.EOF
	}
	n = copy(p, r.buff)
	r.buff = r.buff[n:]
	return
}

func (r *mockBody) Sum32() uint32 {
	crc := crc32.NewIEEE()
	crc.Write(r.buff[0:len(r.buff)])
	return crc.Sum32()
}

func newMockBody(size int) *mockBody {
	buff := make([]byte, size)
	crand.Read(buff)
	return &mockBody{
		buff: buff,
	}
}

func TestCreateUserRouteInfo(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	uid := UserID{ID: "create-user-id"}

	{
		require.ErrorIs(t, d.createUserRoute(Ctx, uid), sdk.ErrNoCluster)
	}

	d.clusters = []string{"1", "2"}
	{
		node.ClusterMgr.EXPECT().GetCluster(A).Return(nil)
		require.ErrorIs(t, d.createUserRoute(Ctx, uid), sdk.ErrNoCluster)
	}

	node.ClusterMgr.EXPECT().GetCluster(A).Return(node.Cluster).AnyTimes()
	{
		node.Cluster.EXPECT().ListVols().Return(nil)
		require.ErrorIs(t, d.createUserRoute(Ctx, uid), sdk.ErrNoVolume)
	}

	node.Cluster.EXPECT().ListVols().Return([]*sdk.VolInfo{
		{Name: "1", Weight: 10},
		{Name: "2", Weight: 90},
	}).AnyTimes()
	{
		node.Cluster.EXPECT().GetVol(A).Return(nil)
		require.ErrorIs(t, d.createUserRoute(Ctx, uid), sdk.ErrNoVolume)
	}

	node.Cluster.EXPECT().GetVol(A).Return(node.Volume).AnyTimes()
	{
		node.Volume.EXPECT().GetDirSnapshot(A, A).Return(nil, e1)
		require.ErrorIs(t, d.createUserRoute(Ctx, uid), e1)
	}

	node.Volume.EXPECT().GetDirSnapshot(A, A).Return(node.Volume, nil).AnyTimes()
	{
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		require.ErrorIs(t, d.createUserRoute(Ctx, uid), e1)
	}

	// public
	uid.Public = true
	{
		err := d.createUserRoute(Ctx, uid)
		require.Error(t, err)
		require.Equal(t, sdk.ErrNotFound.Code, err.(*sdk.Error).Code, uid.String())
	}
	d.publicApps = map[string]AppVolume{uid.ID: {ClusterID: "cluster", VolumeID: "volume"}}

	node.Volume.EXPECT().Lookup(A, A, A).DoAndReturn(
		func(_ context.Context, _ uint64, name string) (*sdk.DirInfo, error) {
			return &sdk.DirInfo{
				Name:  name,
				Type:  uint32(fs.ModeDir),
				Inode: node.GenInode(),
			}, nil
		}).AnyTimes()
	{
		node.Volume.EXPECT().CreateFile(A, A, A).Return(nil, uint64(0), e2)
		require.ErrorIs(t, d.createUserRoute(Ctx, uid), e2)
	}
	{
		node.Volume.EXPECT().CreateFile(A, A, A).Return(nil, uint64(0), sdk.ErrExist)
		node.Volume.EXPECT().GetXAttr(A, A, A).Return("", e3)
		require.ErrorIs(t, d.createUserRoute(Ctx, uid), e3)
	}
	{
		node.Volume.EXPECT().CreateFile(A, A, A).Return(nil, uint64(0), sdk.ErrExist)
		node.Volume.EXPECT().GetXAttr(A, A, A).Return("{json", nil)
		require.Error(t, d.createUserRoute(Ctx, uid), "json unmarshal")
	}

	node.Volume.EXPECT().CreateFile(A, A, A).DoAndReturn(
		func(_ context.Context, parentIno uint64, name string) (*sdk.InodeInfo, uint64, error) {
			return &sdk.InodeInfo{
				Inode: parentIno + 1,
				Mode:  uint32(os.ModeIrregular),
			}, 0, nil
		}).Times(3)
	{
		node.Volume.EXPECT().SetXAttrNX(A, A, A, A).Return(e4)
		require.ErrorIs(t, d.createUserRoute(Ctx, uid), e4)
	}
	{
		node.Volume.EXPECT().SetXAttrNX(A, A, A, A).Return(sdk.ErrExist)
		require.NoError(t, d.createUserRoute(Ctx, uid))
	}
	{
		node.Volume.EXPECT().SetXAttrNX(A, A, A, A).Return(nil)
		require.NoError(t, d.createUserRoute(Ctx, uid))
	}
}

func TestGetUserRouteInfo(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode

	node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
	_, err := d.GetUserRouteInfo(Ctx, testUserID)
	require.Equal(t, err.Error(), e1.Message)

	node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
	_, err = d.GetUserRouteInfo(Ctx, testUserID)
	require.ErrorIs(t, err, sdk.ErrNoCluster)

	node.AnyLookup()
	node.Volume.EXPECT().GetXAttr(A, A, A).Return("", sdk.ErrNotFound)
	_, err = d.GetUserRouteInfo(Ctx, testUserID)
	require.ErrorIs(t, err, sdk.ErrNotFound)

	node.Volume.EXPECT().GetXAttr(A, A, A).Return("", nil)
	_, err = d.GetUserRouteInfo(Ctx, testUserID)
	require.ErrorIs(t, err, sdk.ErrNoCluster)

	node.Volume.EXPECT().GetXAttr(A, A, A).Return("{", nil)
	_, err = d.GetUserRouteInfo(Ctx, testUserID)
	require.Equal(t, err.Error(), "internal server error : unexpected end of JSON input")

	testUser1 := UserID{ID: "test1"}
	ur := UserRoute{
		Uid:         "test1",
		ClusterType: 1,
		ClusterID:   "cluster01",
		VolumeID:    "volume01",
		DriveID:     "test1_drive",
		RootPath:    getRootPath(testUser1),
		RootFileID:  10,
		Ctime:       time.Now().Unix(),
	}
	v, _ := json.Marshal(ur)
	node.Volume.EXPECT().GetXAttr(A, A, A).Return(string(v), nil)
	ur1, err := d.GetUserRouteInfo(Ctx, testUser1)
	require.NoError(t, err)
	require.Equal(t, *ur1, ur)
}

func TestGetUserRouterAndVolume(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	testUserID1 := UserID{ID: "test1"}

	const uid = "test1"

	node.AnyLookup()
	node.Volume.EXPECT().GetXAttr(A, A, A).DoAndReturn(func(context.Context, uint64, string) (string, error) {
		ur := UserRoute{
			Uid:         uid,
			ClusterType: 1,
			ClusterID:   "cluster01",
			VolumeID:    "volume01",
			DriveID:     "test1_drive",
			RootPath:    getRootPath(testUserID1),
			RootFileID:  10,
			Ctime:       time.Now().Unix(),
		}
		v, _ := json.Marshal(ur)
		return string(v), nil
	}).AnyTimes()

	node.ClusterMgr.EXPECT().GetCluster(A).Return(nil)
	_, _, err := d.getUserRouterAndVolume(Ctx, testUserID1)
	require.ErrorIs(t, err, sdk.ErrNoCluster)

	node.ClusterMgr.EXPECT().GetCluster(A).Return(node.Cluster).AnyTimes()
	node.Cluster.EXPECT().GetVol(A).Return(nil)
	_, _, err = d.getUserRouterAndVolume(Ctx, testUserID1)
	require.ErrorIs(t, err, sdk.ErrNoVolume)

	node.Cluster.EXPECT().GetVol(A).Return(node.Volume).AnyTimes()
	node.Volume.EXPECT().GetDirSnapshot(A, A).Return(nil, e1)
	_, _, err = d.getUserRouterAndVolume(Ctx, testUserID1)
	require.ErrorIs(t, err, e1)

	node.Volume.EXPECT().GetDirSnapshot(A, A).Return(node.Volume, nil)
	ur, vol, err := d.getUserRouterAndVolume(Ctx, testUserID1)
	require.NoError(t, err)
	require.Equal(t, ur.RootFileID, Inode(10))
	require.Equal(t, vol, node.Volume)
}

func TestLookup(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode

	_, err := d.lookup(Ctx, node.Volume, 1, "/")
	require.ErrorIs(t, err, sdk.ErrBadRequest)

	node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
	_, err = d.lookup(Ctx, node.Volume, 1, "/a/")
	require.ErrorIs(t, err, sdk.ErrNotFound)

	node.LookupN(3)
	dirInfo, err := d.lookup(Ctx, node.Volume, 1, "/a/b/c")
	require.ErrorIs(t, err, nil)
	require.Equal(t, dirInfo.Name, "c")
	require.Equal(t, dirInfo.Inode, uint64(4))
}

func TestCreateDir(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode

	for _, path := range []string{"", "/"} {
		ino, _, err := d.createDir(Ctx, node.Volume, 1, path, false)
		require.NoError(t, err)
		require.Equal(t, uint64(1), ino.Uint64())
	}

	node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound).Times(2)
	_, _, err := d.createDir(Ctx, node.Volume, 1, "/a/b", false)
	require.ErrorIs(t, err, sdk.ErrNotFound)
	node.Volume.EXPECT().Mkdir(A, A, A).Return(nil, uint64(0), sdk.ErrForbidden)
	_, _, err = d.createDir(Ctx, node.Volume, 1, "/a", false)
	require.ErrorIs(t, err, sdk.ErrForbidden)

	node.OnceLookup(false)
	_, _, err = d.createDir(Ctx, node.Volume, 1, "/a/b", false)
	require.ErrorIs(t, err, sdk.ErrConflict)

	node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound).Times(2)
	node.Volume.EXPECT().Mkdir(A, A, A).Return(nil, uint64(0), sdk.ErrExist)
	_, _, err = d.createDir(Ctx, node.Volume, 1, "/a", true)
	require.ErrorIs(t, err, sdk.ErrNotFound)

	node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
	node.Volume.EXPECT().Mkdir(A, A, A).Return(nil, uint64(0), sdk.ErrExist)
	node.OnceLookup(false)
	_, _, err = d.createDir(Ctx, node.Volume, 1, "/a", true)
	require.ErrorIs(t, err, sdk.ErrConflict)

	node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
	node.Volume.EXPECT().Mkdir(A, A, A).Return(nil, uint64(0), sdk.ErrExist)
	node.OnceLookup(true)
	_, _, err = d.createDir(Ctx, node.Volume, 1, "/a", true)
	require.NoError(t, err)

	node.LookupDirN(2)
	node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound)
	node.Volume.EXPECT().Mkdir(A, A, A).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, uint64, error) {
			return &sdk.InodeInfo{
				Inode:      parentIno + 1,
				Mode:       uint32(os.ModeDir),
				Nlink:      1,
				Size:       4096,
				ModifyTime: time.Now(),
				CreateTime: time.Now(),
				AccessTime: time.Now(),
			}, 0, nil
		})
	_, _, err = d.createDir(Ctx, node.Volume, 1, "/a/b/c", false)
	require.NoError(t, err)

	node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotFound).Times(3)
	node.Volume.EXPECT().Mkdir(A, A, A).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, uint64, error) {
			return &sdk.InodeInfo{
				Inode:      parentIno + 1,
				Mode:       uint32(os.ModeDir),
				Nlink:      1,
				Size:       4096,
				ModifyTime: time.Now(),
				CreateTime: time.Now(),
				AccessTime: time.Now(),
			}, 0, nil
		}).Times(3)
	_, _, err = d.createDir(Ctx, node.Volume, 1, "/a/b/c", true)
	require.Nil(t, err)

	node.LookupDirN(3)
	_, _, err = d.createDir(Ctx, node.Volume, 1, "/a/b/c", true)
	require.Nil(t, err)
}

func TestCreateFile(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode

	_, _, err := d.createFile(Ctx, node.Volume, 1, "/")
	require.ErrorIs(t, err, sdk.ErrBadRequest)

	node.Volume.EXPECT().CreateFile(A, A, A).Return(nil, uint64(0), sdk.ErrBadFile)
	_, _, err = d.createFile(Ctx, node.Volume, 1, "a")
	require.ErrorIs(t, err, sdk.ErrBadFile)

	node.Volume.EXPECT().CreateFile(A, A, A).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, uint64, error) {
			return &sdk.InodeInfo{
				Inode: parentIno + 1,
				Mode:  uint32(os.ModeIrregular),
			}, 0, nil
		})
	inoInfo, _, err := d.createFile(Ctx, node.Volume, 1, "a")
	require.Nil(t, err)
	require.Equal(t, inoInfo.Inode, uint64(2))

	node.LookupDirN(2)
	node.OnceGetInode()
	node.Volume.EXPECT().CreateFile(A, A, A).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, uint64, error) {
			return &sdk.InodeInfo{
				Inode: parentIno + 1,
				Mode:  uint32(os.ModeIrregular),
				Size:  100,
			}, 0, nil
		})
	inoInfo, _, err = d.createFile(Ctx, node.Volume, 1, "/a/b/c")
	require.Nil(t, err)
	require.Equal(t, inoInfo.Size, uint64(100))

	node.LookupDirN(2)
	node.OnceLookup(false)
	node.Volume.EXPECT().CreateFile(A, A, A).Return(nil, uint64(0), sdk.ErrExist)
	_, _, err = d.createFile(Ctx, node.Volume, 1, "/a/b/c")
	require.NoError(t, err)
}

func TestInitClusterConfig(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode

	node.Volume.EXPECT().Lookup(A, A, A).Return(nil, sdk.ErrNotDir)
	require.ErrorIs(t, d.initClusterConfig(), sdk.ErrNotDir)

	node.AnyLookup()
	node.Volume.EXPECT().GetInode(A, A).Return(nil, sdk.ErrNotFound)
	require.ErrorIs(t, d.initClusterConfig(), sdk.ErrNotFound)

	node.OnceGetInode()
	node.Volume.EXPECT().ReadFile(A, A, A, A).Return(0, sdk.ErrUnauthorized)
	require.ErrorIs(t, d.initClusterConfig(), sdk.ErrUnauthorized)

	node.OnceGetInode()
	node.Volume.EXPECT().ReadFile(A, A, A, A).DoAndReturn(
		func(ctx context.Context, inode uint64, off uint64, data []byte) (n int, err error) {
			copy(data, []byte("1234567890"))
			return 10, nil
		})
	require.Error(t, d.initClusterConfig())

	cfg := ClusterConfig{}
	cfg.PublicApps = make(map[string]AppVolume)
	for i := 0; i < 5; i++ {
		cfg.Clusters = append(cfg.Clusters, ClusterInfo{
			ClusterID: fmt.Sprintf("%d", i+1),
			Master:    "127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002",
			Priority:  10,
		})
		cfg.PublicApps[fmt.Sprintf("public-%d", i)] = AppVolume{
			ClusterID: fmt.Sprintf("%d", i+1),
			Master:    "127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002",
			VolumeID:  fmt.Sprintf("%d", i+1),
		}
	}
	data, _ := json.Marshal(&cfg)
	node.Volume.EXPECT().GetInode(A, A).DoAndReturn(
		func(ctx context.Context, inode uint64) (*proto.InodeInfo, error) {
			return &proto.InodeInfo{
				Inode: inode,
				Mode:  uint32(os.ModeIrregular),
				Size:  uint64(len(data)),
			}, nil
		})
	node.Volume.EXPECT().ReadFile(A, A, A, A).DoAndReturn(
		func(ctx context.Context, inode uint64, off uint64, b []byte) (n int, err error) {
			copy(b, data)
			return len(data), nil
		})
	clusterIDs := []string{}
	masters := []string{}
	node.ClusterMgr.EXPECT().AddCluster(A, A, A).DoAndReturn(
		func(ctx context.Context, clusterid string, master string) error {
			for _, _clusterid := range clusterIDs {
				if _clusterid == clusterid {
					return nil
				}
			}
			clusterIDs = append(clusterIDs, clusterid)
			masters = append(masters, master)
			return nil
		}).Times(10)
	require.NoError(t, d.initClusterConfig())
	require.Equal(t, len(clusterIDs), 5)
	require.Equal(t, len(masters), 5)
	for i := 0; i < 5; i++ {
		require.Equal(t, clusterIDs[i], cfg.Clusters[i].ClusterID)
		require.Equal(t, masters[i], cfg.Clusters[i].Master)
	}
}
