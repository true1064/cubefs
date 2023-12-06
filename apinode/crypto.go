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
	errNew    = sdk.ErrTransCipher.Extend("new cipher")
	errQuery  = sdk.ErrTransCipher.Extend("decode query")
	errHeader = sdk.ErrTransCipher.Extend("decode header")
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
	if isLocalRequest(req) {
		f(w, req)
		return
	}
	span := trace.SpanFromContextSafe(req.Context())

	material := req.Header.Get(drive.HeaderCipherMaterial)
	var metaMaterial, bodyMaterial string
	if len(material) > 0 {
		//    1-bit          1-bit     1-bit
		// response-body  request-body  meta
		char := material[0]

		switch char {
		case '1', '3', '5', '7':
			metaMaterial = material[1:]
		}
		switch char {
		case '2', '3', '6', '7':
			bodyMaterial = material[1:]
		}
		switch char {
		case '4', '5', '6', '7':
			req.Header.Set(drive.HeaderCipherMaterial, material[1:])
		default:
			req.Header.Del(drive.HeaderCipherMaterial)
		}
	}

	var err error
	defer func() {
		if err == nil {
			return
		}

		span.Warn(drive.HeaderCipherMaterial, material)
		handleCounter("crypto", req.Method, sdk.ErrTransCipher.Status)

		w.Header().Set(trace.GetTraceIDKey(), span.TraceID())
		replyWithError(w, err)
	}()

	st := time.Now()
	if bodyMaterial != "" {
		var decryptBody io.Reader
		if decryptBody, err = c.cryptor.TransDecryptor(bodyMaterial, req.Body); err != nil {
			span.Warn("new request trans:", err.Error())
			err = errNew
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
		span.Warn("new trans:", err.Error())
		err = errNew
		return
	}
	if metaMaterial != "" {
		span.AppendTrackLog("tnq", st, nil)
	}

	st = time.Now()
	querys := req.URL.Query()
	for key := range querys {
		value := querys.Get(key)
		if len(value) == 0 {
			continue
		}

		var val string
		if val, err = t.Decrypt(value, true); err != nil {
			span.Warnf("decode query %s %s: %s", key, value, err.Error())
			err = errQuery
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
			span.Warnf("decode header key %s: %s", key, err.Error())
			err = errHeader
			return
		}
		if v, err = t.Decrypt(req.Header.Get(key), true); err != nil {
			span.Warnf("decode header val %s %s: %s", k, req.Header.Get(key), err.Error())
			err = errHeader
			return
		}
		req.Header.Del(key)
		req.Header.Set(drive.EncodeMetaHeader(k), drive.EncodeMeta(v))
	}
	if metaMaterial != "" {
		span.AppendTrackLog("tdq", st, nil)
	}

	f(w, req)
}
