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
	"time"

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
	var span trace.Span
	if rid := req.Header.Get(drive.HeaderRequestID); rid != "" {
		span, _ = trace.StartSpanFromContextWithTraceID(req.Context(), "", rid)
	} else {
		span = trace.SpanFromContextSafe(req.Context())
	}

	metaMaterial := req.Header.Get(drive.HeaderCipherMeta)
	bodyMaterial := req.Header.Get(drive.HeaderCipherBody)

	var err error
	var errBuff []byte
	defer func() {
		if err == nil {
			return
		}

		span.Warn(err)
		span.Warn(drive.HeaderCipherMeta, metaMaterial)
		span.Warn(drive.HeaderCipherBody, bodyMaterial)
		handleCounter("crypto", req.Method, sdk.ErrTransCipher.Status)

		w.Header().Set(trace.GetTraceIDKey(), span.TraceID())
		w.Header().Set(rpc.HeaderContentType, rpc.MIMEJSON)
		w.Header().Set(rpc.HeaderContentLength, fmt.Sprint(len(errBuff)))

		w.WriteHeader(sdk.ErrTransCipher.Status)
		w.Write(errBuff)
	}()

	st := time.Now()
	if bodyMaterial != "" {
		var decryptBody io.Reader
		if decryptBody, err = c.cryptor.TransDecryptor(bodyMaterial, req.Body); err != nil {
			err = fmt.Errorf("new request trans: %s", err.Error())
			errBuff = errNew[:]
			return
		}
		span.AppendTrackLog("tnb", st, nil)

		req.Body = requestBody{
			Reader: decryptBody,
			Closer: req.Body,
		}
	}

	st = time.Now()
	var t crypto.Transmitter
	if t, err = c.cryptor.Transmitter(metaMaterial); err != nil {
		err = fmt.Errorf("new trans: %s", err.Error())
		errBuff = errNew[:]
		return
	}
	span.AppendTrackLog("tnq", st, nil)

	st = time.Now()
	querys := req.URL.Query()
	for key := range querys {
		value := querys.Get(key)
		if len(value) == 0 {
			continue
		}

		var val string
		if val, err = t.Decrypt(value, true); err != nil {
			err = fmt.Errorf("decode query %s %s: %s", key, value, err.Error())
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
		req.Header.Del(key)
		req.Header.Set(drive.EncodeMetaHeader(k), drive.EncodeMeta(v))
	}
	span.AppendTrackLog("tdq", st, nil)

	st = time.Now()
	f(w, req)
	span.AppendTrackLog("c", st, nil)
}
