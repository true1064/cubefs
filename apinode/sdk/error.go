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

package sdk

import (
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

// Error implemente for drive, s3, posix, hdfs.
type Error struct {
	Status  int
	Code    string
	Message string
}

var _ rpc.HTTPError = &Error{}

// StatusCode implemented rpc.HTTPError.
func (e *Error) StatusCode() int {
	return e.Status
}

// ErrorCode implemented rpc.HTTPError.
func (e *Error) ErrorCode() string {
	return e.Code
}

// Error implemented rpc.HTTPError.
func (e *Error) Error() string {
	return e.Message
}

// defined errors.
var (
	ErrBadRequest   = &Error{Status: 400, Code: "BadRequest", Message: "bad request"}
	ErrUnauthorized = &Error{Status: 401, Code: "Unauthorized", Message: "unauthorized"}
	ErrForbidden    = &Error{Status: 403, Code: "Forbidden", Message: "dorbidden"}
	ErrNotFound     = &Error{Status: 404, Code: "NotFound", Message: "not found"}

	ErrInternalServerError = &Error{Status: 500, Code: "InternalServerError", Message: "internal server error"}
	ErrBadGateway          = &Error{Status: 502, Code: "BadGateway", Message: "bad gateway"}
)
