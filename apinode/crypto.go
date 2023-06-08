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

package apinode

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/cubefs/cubefs/apinode/crypto"
	"github.com/cubefs/cubefs/apinode/drive"
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const metaHeaderLen = len(drive.UserPropertyPrefix)

var (
	errNew    = []byte(`{"code":"TransCipher","error":"trans new cipher"}`)
	errQuery  = []byte(`{"code":"TransCipher","error":"trans decode query"}`)
	errHeader = []byte(`{"code":"TransCipher","error":"trans decode header"}`)
)

type requestBody struct {
	io.Reader
	io.Closer
}

type cryptor struct {
	cryptor crypto.Cryptor
}

func newCryptor() rpc.ProgressHandler {
	return &cryptor{cryptor: crypto.NewCryptor()}
}

// only decode query string and meta headers.
func (c cryptor) Handler(w http.ResponseWriter, req *http.Request, f func(http.ResponseWriter, *http.Request)) {
	material := req.Header.Get(drive.HeaderCipherMaterial)
	if material == "" {
		f(w, req)
		return
	}

	var err error
	var errBuff []byte
	defer func() {
		if err == nil {
			return
		}

		var span trace.Span
		if rid := req.Header.Get(drive.HeaderRequestID); rid != "" {
			span, _ = trace.StartSpanFromContextWithTraceID(req.Context(), "", rid)
		} else {
			span = trace.SpanFromContextSafe(req.Context())
		}
		span.Warn(err)

		w.Header().Set(trace.GetTraceIDKey(), span.TraceID())
		w.Header().Set(rpc.HeaderContentType, rpc.MIMEJSON)
		w.Header().Set(rpc.HeaderContentLength, fmt.Sprint(len(errBuff)))

		w.WriteHeader(sdk.ErrTransCipher.Status)
		w.Write(errBuff)
	}()

	var t crypto.Transmitter
	if t, err = c.cryptor.DecryptTransmitter(material); err != nil {
		err = fmt.Errorf("new trans: %s", err.Error())
		errBuff = errNew[:]
		return
	}

	req.Body = requestBody{
		Reader: t.Transmit(req.Body),
		Closer: req.Body,
	}

	querys := req.URL.Query()
	for key := range querys {
		var val string
		if val, err = t.Decrypt(querys.Get(key), true); err != nil {
			err = fmt.Errorf("decode query %s %s: %s", key, querys.Get(key), err.Error())
			errBuff = errQuery[:]
			return
		}
		querys.Set(key, val)
	}
	req.URL.RawQuery = querys.Encode()

	metas := make([]string, 0, 4)
	for key := range req.Header {
		key = strings.ToLower(key)
		if len(key) > metaHeaderLen && strings.HasPrefix(key, drive.UserPropertyPrefix) {
			metas = append(metas, key)
		}
	}

	for _, key := range metas {
		var k, v string
		if k, err = t.Decrypt(key[metaHeaderLen:], true); err != nil {
			err = fmt.Errorf("decode header key %s: %s", key, err.Error())
			errBuff = errHeader[:]
			return
		}
		if v, err = t.Decrypt(req.Header.Get(key), true); err != nil {
			err = fmt.Errorf("decode header val %s %s: %s", k, req.Header.Get(key), err.Error())
			errBuff = errHeader[:]
			return
		}
		req.Header.Set(drive.UserPropertyPrefix+k, v)
		req.Header.Del(key)
	}

	f(w, req)
}
