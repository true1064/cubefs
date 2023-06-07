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
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/cubefs/cubefs/apinode/crypto"
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// RegisterAPIRouters register drive api handler.
func (d *DriveNode) RegisterAPIRouters() *rpc.Router {
	rpc.RegisterArgsParser(&ArgsListDir{}, "json")
	rpc.RegisterArgsParser(&ArgsAddUserConfig{}, "json")
	rpc.RegisterArgsParser(&ArgsSetProperties{}, "json")
	rpc.RegisterArgsParser(&ArgsDelProperties{}, "json")
	rpc.RegisterArgsParser(&ArgsGetProperties{}, "json")
	rpc.RegisterArgsParser(&ArgsMkDir{}, "json")

	rpc.RegisterArgsParser(&ArgsFileUpload{}, "json")
	rpc.RegisterArgsParser(&ArgsFileWrite{}, "json")
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
	r.Handle(http.MethodGet, "/v1/drive", d.handleGetDrive)

	r.Handle(http.MethodPut, "/v1/user/config", d.handleAddUserConfig, rpc.OptArgsQuery())
	r.Handle(http.MethodDelete, "/v1/user/config", d.handleDelUserConfig, rpc.OptArgsQuery())
	r.Handle(http.MethodGet, "/v1/user/config", d.handleGetUserConfig)

	r.Handle(http.MethodGet, "/v1/files", d.handleListDir, rpc.OptArgsQuery())
	r.Handle(http.MethodDelete, "/v1/files", d.handleFilesDelete, rpc.OptArgsQuery())
	r.Handle(http.MethodPost, "/v1/files/mkdir", d.handleMkDir, rpc.OptArgsQuery())

	// file
	r.Handle(http.MethodPut, "/v1/files/upload", d.handleFileUpload, rpc.OptArgsQuery())
	r.Handle(http.MethodPost, "/v1/files/upload", d.handleFileUpload, rpc.OptArgsQuery())
	r.Handle(http.MethodPut, "/v1/files/content", d.handleFileWrite, rpc.OptArgsQuery())
	r.Handle(http.MethodGet, "/v1/files/content", d.handleFileDownload, rpc.OptArgsQuery())
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
	rid := c.Request.Header.Get(headerRequestID)
	c.Set(headerRequestID, rid)

	uid := UserID(c.Request.Header.Get(headerUserID))
	if !uid.Valid() {
		c.AbortWithError(sdk.ErrBadRequest)
		return
	}
	c.Set(headerUserID, uid)

	// pre-set encrypt transmitter
	t, err := d.cryptor.EncryptTransmitter(c.Request.Header.Get(headerCipherMaterial))
	if err != nil {
		c.AbortWithError(err)
		return
	}
	c.Set(headerCipherMaterial, t)
}

func (*DriveNode) requestID(c *rpc.Context) string {
	rid, _ := c.Get(headerRequestID)
	return rid.(string)
}

func (*DriveNode) userID(c *rpc.Context) UserID {
	uid, _ := c.Get(headerUserID)
	return uid.(UserID)
}

func (*DriveNode) encrypTransmitter(c *rpc.Context) crypto.Transmitter {
	t, _ := c.Get(headerCipherMaterial)
	return t.(crypto.Transmitter)
}

func (d *DriveNode) decryptTransmitter(c *rpc.Context) crypto.Transmitter {
	t, err := d.cryptor.DecryptTransmitter(c.Request.Header.Get(headerCipherMaterial))
	if err != nil {
		_, span := d.ctxSpan(c)
		span.Warn("make decrypt transmitter", err)
		c.RespondError(sdk.ErrTransCipher)
		return nil
	}
	return t
}

// span carry with request id firstly.
func (d *DriveNode) ctxSpan(c *rpc.Context) (context.Context, trace.Span) {
	ctx := c.Request.Context()
	var span trace.Span
	if rid := d.requestID(c); rid != "" {
		span, _ = trace.StartSpanFromContextWithTraceID(ctx, "", rid)
	} else {
		span = trace.SpanFromContextSafe(ctx)
	}
	return ctx, span
}

func (d *DriveNode) getProperties(c *rpc.Context) (map[string]string, error) {
	t := d.encrypTransmitter(c)
	properties := make(map[string]string)
	for key, values := range c.Request.Header {
		key = strings.ToLower(key)
		if len(key) > len(userPropertyPrefix) && strings.HasPrefix(key, userPropertyPrefix) {
			k, err := t.Decrypt(key[len(userPropertyPrefix):], true)
			if err != nil {
				return nil, err
			}
			v, err := t.Decrypt(values[0], true)
			if err != nil {
				return nil, err
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
	dataStr, err := d.encrypTransmitter(c).Encrypt(string(buffer), false)
	if err != nil {
		c.RespondError(err)
		return
	}
	c.RespondWith(http.StatusOK, rpc.MIMEJSON, []byte(dataStr))
}

type errorResponse struct {
	Error   string `json:"error"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
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
