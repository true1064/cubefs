package drive

import (
	"net/http"
	"testing"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/stretchr/testify/require"
)

func TestHandleCreateDirSnapshot(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(path, ver string) rpc.HTTPError {
		url := genURL(server.URL, "/v1/snapshot", "path", path, "ver", ver)
		req, _ := http.NewRequest(http.MethodPost, url, nil)
		req.Header.Add(HeaderUserID, testUserID)
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		return resp2Error(resp)
	}

	{
		require.Equal(t, 400, doRequest("../a", "v1").StatusCode())
		require.Equal(t, 400, doRequest("/a", "").StatusCode())
		node.TestGetUser(t, func() rpc.HTTPError { return doRequest("/a", "v1") }, testUserID)
		node.OnceGetUser()
		node.Volume.EXPECT().CreateDirSnapshot(A, A, A).Return(e1)
		require.Equal(t, e1.Status, doRequest("/a", "v1").StatusCode())
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().CreateDirSnapshot(A, A, A).Return(nil)
		require.NoError(t, doRequest("/a", "v1"))
	}
}

func TestHandleDeleteDirSnapshot(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(path, ver string) rpc.HTTPError {
		url := genURL(server.URL, "/v1/snapshot", "path", path, "ver", ver)
		req, _ := http.NewRequest(http.MethodDelete, url, nil)
		req.Header.Add(HeaderUserID, testUserID)
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		defer resp.Body.Close()
		return resp2Error(resp)
	}

	{
		require.Equal(t, 400, doRequest("../a", "v1").StatusCode())
		require.Equal(t, 400, doRequest("/a", "").StatusCode())
		node.TestGetUser(t, func() rpc.HTTPError { return doRequest("/a", "v1") }, testUserID)
		node.OnceGetUser()
		node.Volume.EXPECT().DeleteDirSnapshot(A, A, A).Return(e1)
		require.Equal(t, e1.Status, doRequest("/a", "v1").StatusCode())
	}
	{
		node.OnceGetUser()
		node.Volume.EXPECT().DeleteDirSnapshot(A, A, A).Return(nil)
		require.NoError(t, doRequest("/a", "v1"))
	}
}
