package drive

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/stretchr/testify/require"
)

func TestHandleSetProperties(t *testing.T) {
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(path string, metas ...string) rpc.HTTPError {
		url := genURL(server.URL, "/v1/files/properties", "path", path)
		req, _ := http.NewRequest(http.MethodPut, url, nil)
		req.Header.Add(HeaderUserID, testUserID)
		for ii := 0; ii < len(metas); ii += 2 {
			req.Header.Add(EncodeMetaHeader(metas[ii]), EncodeMeta(metas[ii+1]))
		}
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		return resp2Error(resp)
	}

	{
		url := genURL(server.URL, "/v1/files/properties", "path", "a")
		req, _ := http.NewRequest(http.MethodPut, url, nil)
		req.Header.Add(HeaderUserID, testUserID)
		req.Header.Add(UserPropertyPrefix+"not-hex-key", "value")
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		require.Equal(t, 400, resp2Error(resp).StatusCode())
		require.Equal(t, 400, doRequest("").StatusCode())
		require.Equal(t, 400, doRequest("../a", "k1", "v1").StatusCode())
	}
	{
		node.TestGetUser(t, func() rpc.HTTPError { return doRequest("a", "k1", "v1") }, testUserID)
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		require.Equal(t, e1.Status, doRequest("a", "k1", "v1").StatusCode())
	}
	{
		node.OnceGetUser()
		node.OnceLookup(false)
		node.Volume.EXPECT().BatchSetXAttr(A, A, A).Return(e2)
		require.Equal(t, e2.Status, doRequest("a", "k1", "v1").StatusCode())
	}
	{
		require.NoError(t, doRequest("a"))
		node.OnceGetUser()
		node.OnceLookup(false)
		node.Volume.EXPECT().BatchSetXAttr(A, A, A).Return(nil)
		require.NoError(t, doRequest("a", "k1", "v1", "k2", "v2"))
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
		req.Header.Add(HeaderUserID, testUserID)
		for _, k := range keys {
			v := ""
			if len(k)%2 == 0 {
				v = "1"
			}
			req.Header.Add(EncodeMetaHeader(k), EncodeMeta(v))
		}
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		return resp2Error(resp)
	}

	{
		url := genURL(server.URL, "/v1/files/properties", "path", "a")
		req, _ := http.NewRequest(http.MethodDelete, url, nil)
		req.Header.Add(HeaderUserID, testUserID)
		req.Header.Add(UserPropertyPrefix+"not-hex-key", "value")
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		require.Equal(t, 400, resp2Error(resp).StatusCode())
	}
	{
		url := genURL(server.URL, "/v1/files/properties", "path", "a")
		req, _ := http.NewRequest(http.MethodDelete, url, nil)
		req.Header.Add(HeaderUserID, testUserID)
		req.Header.Add(EncodeMetaHeader("hex-key"), "not-hex-value")
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		require.Equal(t, 400, resp2Error(resp).StatusCode())
	}
	{
		require.Equal(t, 400, doRequest("").StatusCode())
		require.Equal(t, 400, doRequest("a/b/../../..").StatusCode())
		longKey := make([]byte, 1025)
		for i := range longKey {
			longKey[i] = 'x'
		}
		require.Equal(t, 400, doRequest("a", string(longKey)).StatusCode())
		keys := make([]string, 17)
		for i := range keys {
			keys[i] = fmt.Sprint("keys-", i)
		}
		require.Equal(t, 400, doRequest("a", keys...).StatusCode())
	}
	{
		require.NoError(t, doRequest("a"))
		require.NoError(t, doRequest("a"))
		require.NoError(t, doRequest("a", "", ""))
	}
	{
		node.TestGetUser(t, func() rpc.HTTPError { return doRequest("a", "k1") }, testUserID)
		node.OnceGetUser()
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
	node := newMockNode(t)
	d := node.DriveNode
	server, client := newTestServer(d)
	defer server.Close()

	doRequest := func(path string, data interface{}) rpc.HTTPError {
		url := genURL(server.URL, "/v1/files/properties", "path", path)
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		req.Header.Add(HeaderUserID, testUserID)
		resp, err := client.Do(Ctx, req)
		require.NoError(t, err)
		return resp2Data(resp, data)
	}

	{
		require.Equal(t, 400, doRequest("", nil).StatusCode())
		require.Equal(t, 400, doRequest("../a", nil).StatusCode())
	}
	{
		node.TestGetUser(t, func() rpc.HTTPError { return doRequest("a", nil) }, testUserID)
		node.OnceGetUser()
		node.Volume.EXPECT().Lookup(A, A, A).Return(nil, e1)
		require.Equal(t, e1.Status, doRequest("a", nil).StatusCode())
	}
	{
		node.OnceGetUser()
		node.OnceLookup(false)
		node.Volume.EXPECT().GetXAttrMap(A, A).Return(nil, e2)
		require.Equal(t, e2.Status, doRequest("a", nil).StatusCode())
	}
	{
		node.OnceGetUser()
		node.OnceLookup(false)
		node.Volume.EXPECT().GetXAttrMap(A, A).Return(nil, nil)
		node.Volume.EXPECT().GetInode(A, A).Return(nil, e3)
		require.Equal(t, e3.Status, doRequest("a", nil).StatusCode())
	}
	{
		node.OnceGetUser()
		node.OnceLookup(false)
		node.Volume.EXPECT().GetXAttrMap(A, A).Return(map[string]string{"k1": "v1"}, nil)
		node.OnceGetInode()
		var rst GetPropertiesResult
		require.NoError(t, doRequest("a", &rst))
		require.Equal(t, typeFile, rst.Type)
		require.Equal(t, "v1", rst.Properties["k1"])
	}
	{
		node.OnceGetUser()
		node.OnceLookup(true)
		node.Volume.EXPECT().GetXAttrMap(A, A).Return(map[string]string{"k1": "v1", "k2": "v2"}, nil)
		node.OnceGetInode()
		var rst GetPropertiesResult
		require.NoError(t, doRequest("a", &rst))
		require.Equal(t, typeFolder, rst.Type)
		require.Equal(t, "v2", rst.Properties["k2"])
	}
}
