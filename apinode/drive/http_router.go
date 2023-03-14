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
	"net/http"
	"strconv"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// RegisterAPIRouters register drive api handler.
func (d *DriveNode) RegisterAPIRouters() *rpc.Router {
	rpc.RegisterArgsParser(&ArgsListDir{}, "json")
	rpc.RegisterArgsParser(&ArgsPath{}, "json")

	r := rpc.New()

	// set request id and user id at interceptors.
	r.Use(d.setHeaders)

	r.Handle(http.MethodPost, "/v1/drive", d.createDrive)

	r.Handle(http.MethodPost, "/v1/route", d.addUserConfig, rpc.OptArgsQuery())
	r.Handle(http.MethodGet, "/v1/route", d.getUserConfig)

	r.Handle(http.MethodPost, "/v1/meta", nil, rpc.OptArgsQuery())
	r.Handle(http.MethodGet, "/v1/meta", nil, rpc.OptArgsQuery())

	r.Handle(http.MethodGet, "/v1/files", d.handlerListDir, rpc.OptArgsQuery())

	return r
}

func (*DriveNode) setHeaders(c *rpc.Context) {
	rid := c.Request.Header.Get(headerRequestID)
	c.Set(headerRequestID, rid)

	uid := c.Request.Header.Get(headerUserID)
	id, err := strconv.Atoi(uid)
	if err != nil || id <= 0 {
		c.AbortWithError(sdk.ErrUnauthorized)
		return
	}
	c.Set(headerUserID, UserID(id))
}

func (*DriveNode) requestID(c *rpc.Context) string {
	rid, _ := c.Get(headerRequestID)
	return rid.(string)
}

func (*DriveNode) userID(c *rpc.Context) UserID {
	uid, _ := c.Get(headerUserID)
	return uid.(UserID)
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
