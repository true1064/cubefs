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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// RegisterAPIRouters register drive api handler.
func (d *DriveNode) RegisterAPIRouters() *rpc.Router {
	rpc.RegisterArgsParser(&ArgsListDir{}, "json")
	rpc.RegisterArgsParser(&ArgsSetProperties{}, "json")
	rpc.RegisterArgsParser(&ArgsDelProperties{}, "json")
	rpc.RegisterArgsParser(&ArgsGetProperties{}, "json")
	rpc.RegisterArgsParser(&ArgsMkDir{}, "json")

	rpc.RegisterArgsParser(&ArgsFileUpload{}, "json")
	rpc.RegisterArgsParser(&ArgsFileWrite{}, "json")
	rpc.RegisterArgsParser(&ArgsFileVerify{}, "json")
	rpc.RegisterArgsParser(&ArgsFileDownload{}, "json")
	rpc.RegisterArgsParser(&ArgsFileRename{}, "json")
	rpc.RegisterArgsParser(&ArgsFileCopy{}, "json")
	rpc.RegisterArgsParser(&ArgsDelete{}, "json")

	rpc.RegisterArgsParser(&ArgsMPUploads{}, "json")
	rpc.RegisterArgsParser(&ArgsMPUpload{}, "json")
	rpc.RegisterArgsParser(&ArgsMPList{}, "json")
	rpc.RegisterArgsParser(&ArgsMPAbort{}, "json")

	r := rpc.New()

	// set request id and user id at interceptors.
	r.Use(d.setHeaders)

	r.Handle(http.MethodPut, "/v1/drive", d.handleCreateDrive)

	r.Handle(http.MethodGet, "/v1/files", d.handleListDir, rpc.OptArgsQuery())
	r.Handle(http.MethodDelete, "/v1/files", d.handleFilesDelete, rpc.OptArgsQuery())
	r.Handle(http.MethodDelete, "/v1/files/batch", d.handleBatchDelete, rpc.OptArgsBody())
	r.Handle(http.MethodPost, "/v1/files/mkdir", d.handleMkDir, rpc.OptArgsQuery())

	// file
	r.Handle(http.MethodPut, "/v1/files/upload", d.handleFileUpload, rpc.OptArgsQuery())
	r.Handle(http.MethodPost, "/v1/files/upload", d.handleFileUpload, rpc.OptArgsQuery())
	r.Handle(http.MethodPut, "/v1/files/content", d.handleFileWrite, rpc.OptArgsQuery())
	r.Handle(http.MethodGet, "/v1/files/content", d.handleFileDownload, rpc.OptArgsQuery())
	r.Handle(http.MethodGet, "/v1/files/verify", d.handleFileVerify, rpc.OptArgsQuery())
	r.Handle(http.MethodPost, "/v1/files/copy", d.handleFileCopy, rpc.OptArgsQuery())
	r.Handle(http.MethodPost, "/v1/files/rename", d.handleFileRename, rpc.OptArgsQuery())
	// file multipart
	r.Handle(http.MethodPost, "/v1/files/multipart", d.handleMultipartUploads, rpc.OptArgsQuery())
	r.Handle(http.MethodPut, "/v1/files/multipart", d.handleMultipartPart, rpc.OptArgsQuery())
	r.Handle(http.MethodGet, "/v1/files/multipart", d.handleMultipartList, rpc.OptArgsQuery())
	r.Handle(http.MethodDelete, "/v1/files/multipart", d.handleMultipartAbort, rpc.OptArgsQuery())

	r.Handle(http.MethodPut, "/v1/files/properties", d.handleSetProperties, rpc.OptArgsQuery())
	r.Handle(http.MethodDelete, "/v1/files/properties", d.handleDelProperties, rpc.OptArgsQuery())
	r.Handle(http.MethodGet, "/v1/files/properties", d.handleGetProperties, rpc.OptArgsQuery())

	return r
}

func (d *DriveNode) setHeaders(c *rpc.Context) {
	rid := c.Request.Header.Get(HeaderRequestID)
	c.Set(HeaderRequestID, rid)

	ctx := c.Request.Context()
	var span trace.Span
	if rid != "" {
		span, _ = trace.StartSpanFromContextWithTraceID(ctx, "", rid)
	} else {
		span = trace.SpanFromContextSafe(ctx)
	}
	c.Set(contextSpan, span)

	uid := UserID(c.Request.Header.Get(HeaderUserID))
	span.Debug("user id:", uid)
	if !uid.Valid() {
		span.Warn("abort invalid user", uid)
		c.AbortWithError(sdk.ErrBadRequest.Extend("invalid user id"))
		return
	}
	c.Set(HeaderUserID, uid)
}

func (*DriveNode) requestID(c *rpc.Context) string {
	rid, _ := c.Get(HeaderRequestID)
	return rid.(string)
}

func (*DriveNode) userID(c *rpc.Context) UserID {
	uid, _ := c.Get(HeaderUserID)
	return uid.(UserID)
}

func (d *DriveNode) encryptResponse(c *rpc.Context, body io.Reader) (io.Reader, error) {
	r, respMaterial, err := d.cryptor.TransEncryptor(c.Request.Header.Get(HeaderCipherBody), body)
	if err != nil {
		_, span := d.ctxSpan(c)
		span.Warn("make encrypt transmitter", err)
		return nil, sdk.ErrTransCipher
	}
	if respMaterial != "" {
		c.Writer.Header().Set(HeaderCipherBody, respMaterial)
	}
	return r, err
}

// span carry with request id firstly.
func (d *DriveNode) ctxSpan(c *rpc.Context) (context.Context, trace.Span) {
	span, ok := c.Get(contextSpan)
	if !ok {
		panic("NOT set context span")
	}
	return c.Request.Context(), span.(trace.Span)
}

func (d *DriveNode) getProperties(c *rpc.Context) (map[string]string, error) {
	properties := make(map[string]string)
	for key := range c.Request.Header {
		key = strings.ToLower(key)
		if len(key) > len(UserPropertyPrefix) && strings.HasPrefix(key, UserPropertyPrefix) {
			k, err := noneTransmitter.Decrypt(key[len(UserPropertyPrefix):], true)
			if err != nil {
				return nil, sdk.ErrBadRequest.Extend("meta key was not invalid hex")
			}
			if strings.HasPrefix(k, internalMetaPrefix) {
				return nil, sdk.ErrBadRequest.Extend("meta key was internal prefix")
			}

			v, err := noneTransmitter.Decrypt(c.Request.Header.Get(key), true)
			if err != nil {
				return nil, sdk.ErrBadRequest.Extend("meta value was not invalid hex")
			}
			if len(k) > 1024 || len(v) > 1024 {
				return nil, sdk.ErrBadRequest.Extend("meta key or value was too long")
			}
			properties[k] = v
			if len(properties) > 16 {
				return nil, sdk.ErrBadRequest.Extend("meta length was more than rated size")
			}
		}
	}
	return properties, nil
}

func (d *DriveNode) respData(c *rpc.Context, obj interface{}) {
	buffer, err := json.Marshal(obj)
	if err != nil {
		c.RespondError(sdk.ErrInternalServerError)
		return
	}
	body, err := d.encryptResponse(c, bytes.NewReader(buffer))
	if err != nil {
		c.RespondError(err)
		return
	}
	c.RespondWithReader(http.StatusOK, len(buffer), rpc.MIMEJSON, body, nil)
}

func (d *DriveNode) respError(c *rpc.Context, err error) {
	c.RespondError(err)
}

func (d *DriveNode) checkError(c *rpc.Context, logger func(error), errs ...error) bool {
	for _, err := range errs {
		if err != nil {
			if logger != nil {
				logger(err)
			}
			d.respError(c, err)
			return true
		}
	}
	return false
}

func (d *DriveNode) checkFunc(c *rpc.Context, logger func(error), funcs ...func() error) bool {
	for _, f := range funcs {
		if err := f(); err != nil {
			if logger != nil {
				logger(err)
			}
			d.respError(c, err)
			return true
		}
	}
	return false
}

// EncodeMeta encode value with hex.
func EncodeMeta(val string) string {
	enVal, _ := noneTransmitter.Encrypt(val, true)
	return enVal
}

// EncodeMetaHeader encode key with hex with const prefix.
func EncodeMetaHeader(key string) string {
	enVal, _ := noneTransmitter.Encrypt(key, true)
	return UserPropertyPrefix + enVal
}
