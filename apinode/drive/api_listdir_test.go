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
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/cubefs/cubefs/apinode/testing/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
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
	mockVol := mocks.NewMockIVolume(ctrl)
	mockClusterMgr := mocks.NewMockClusterManager(ctrl)
	d := &DriveNode{
		vol:        mockVol,
		userRouter: urm,
		clusterMgr: mockClusterMgr,
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
		urm.Set("test", &UserRoute{})

	}
}
