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
	"fmt"
	"net/http"
	"strings"

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

// Extend error with full message.
func (e *Error) Extend(a ...interface{}) *Error {
	return &Error{
		Status:  e.Status,
		Code:    e.Code,
		Message: fmt.Sprintf("%s : %s", e.Error(), strings.TrimRight(fmt.Sprintln(a...), "\n")),
	}
}

// Extendf error with full format message.
func (e *Error) Extendf(format string, a ...interface{}) *Error {
	return &Error{
		Status:  e.Status,
		Code:    e.Code,
		Message: fmt.Sprintf("%s : %s", e.Error(), fmt.Sprintf(format, a...)),
	}
}

// defined errors.
var (
	ErrBadRequest   = &Error{Status: 400, Code: "BadRequest", Message: "bad request"}
	ErrUnauthorized = &Error{Status: 401, Code: "Unauthorized", Message: "unauthorized"}
	ErrForbidden    = &Error{Status: 403, Code: "Forbidden", Message: "forbidden"}
	ErrNotFound     = &Error{Status: 404, Code: "NotFound", Message: "not found"}

	ErrNotDir   = &Error{Status: 452, Code: "NotDir", Message: "not a directory"}
	ErrNotEmpty = &Error{Status: 453, Code: "NotEmpty", Message: "directory not empty"}
	ErrNoUser   = &Error{Status: 454, Code: "NotFoundUser", Message: "not found user"}
	ErrNotFile  = &Error{Status: 456, Code: "NotFile", Message: "not a file"}

	ErrTokenVerify  = &Error{Status: 481, Code: "InvalidToken", Message: "invalid token"}
	ErrTokenExpires = &Error{Status: 482, Code: "TokenExpires", Message: "token expires"}
	ErrAppExit      = &Error{Status: 483, Code: "4042", Message: "all tokens expires"}
	ErrAccExit      = &Error{Status: 484, Code: "4043", Message: "account already exit"}

	ErrInvalidPath      = &Error{Status: 400, Code: "BadRequest", Message: "invalid path"}
	ErrMismatchChecksum = &Error{Status: 461, Code: "MismatchChecksum", Message: "mismatch checksum"}
	ErrTransCipher      = &Error{Status: 462, Code: "TransCipher", Message: "trans cipher"}
	ErrReadOnly         = &Error{Status: 463, Code: "ReadOnly", Message: "write is disabled"}
	ErrWriteOverSize    = &Error{Status: 470, Code: "WriteOverSize", Message: "write over size"}
	ErrServerCipher     = &Error{Status: 500, Code: "ServerCipher", Message: "server cipher"}

	ErrInvalidPartOrder = newErr(http.StatusBadRequest, "request part order is invalid")
	ErrInvalidPart      = newErr(http.StatusBadRequest, "request part is invalid")
	ErrLimitExceed      = newErr(http.StatusTooManyRequests, "request limit exceed")
	ErrConflict         = newErr(http.StatusConflict, "operation conflict")
	ErrExist            = newErr(http.StatusConflict, "file already exist")
	ErrTooLarge         = newErr(http.StatusRequestEntityTooLarge, "request entity too large")

	ErrInternalServerError = &Error{Status: 500, Code: "InternalServerError", Message: "internal server error"}
	ErrBadGateway          = &Error{Status: 502, Code: "BadGateway", Message: "bad gateway"}

	ErrNoLeader   = newErr(http.StatusInternalServerError, "no valid leader")
	ErrNoMaster   = newErr(http.StatusInternalServerError, "no valid master")
	ErrRetryAgain = newErr(http.StatusInternalServerError, "retry again")
	ErrFull       = newErr(http.StatusInternalServerError, "no available resource")
	ErrBadFile    = newErr(http.StatusInternalServerError, "request file handle not exist")
	ErrNoCluster  = newErr(http.StatusInternalServerError, "no valid cluster")
	ErrNoVolume   = newErr(http.StatusInternalServerError, "no valid volume")
)

// newErr with http.statusCode
func newErr(status int, msg string) *Error {
	code := http.StatusText(status)
	if code == "" {
		code = "UnknownErr"
	}

	return &Error{Status: status, Code: code, Message: msg}
}
