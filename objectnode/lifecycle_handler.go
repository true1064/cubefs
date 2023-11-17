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

package objectnode

import (
	"encoding/xml"
	"io"
	"net/http"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// API reference: https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html
func (o *ObjectNode) getBucketLifecycleConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode

	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	if _, err = o.vm.Volume(param.Bucket()); err != nil {
		errorCode = NoSuchBucket
		return
	}

	var lcConf *proto.LcConfiguration
	if lcConf, err = o.mc.AdminAPI().GetBucketLifecycle(param.Bucket()); err != nil {
		log.LogErrorf("getBucketLifecycle failed: bucket[%v] err(%v)", param.Bucket(), err)
		errorCode = NoSuchLifecycleConfiguration
		return
	}

	var lifeCycle = NewLifecycleConfiguration()
	lifeCycle.Rules = lcConf.Rules
	var data []byte
	data, err = xml.Marshal(lifeCycle)
	if err != nil {
		log.LogErrorf("getBucketLifecycle failed: bucket[%v] err(%v)", param.Bucket(), err)
		errorCode = NoSuchLifecycleConfiguration
		return
	}

	writeSuccessResponseXML(w, data)
	return
}

// API reference: https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html
func (o *ObjectNode) putBucketLifecycleConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode

	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	if _, err = o.vm.Volume(param.Bucket()); err != nil {
		errorCode = NoSuchBucket
		return
	}

	_, errorCode = VerifyContentLength(r, BodyLimit)
	if errorCode != nil {
		return
	}
	var requestBody []byte
	if requestBody, err = io.ReadAll(r.Body); err != nil && err != io.EOF {
		log.LogErrorf("putBucketLifecycle failed: read request body data err: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = &ErrorCode{
			ErrorCode:    http.StatusText(http.StatusBadRequest),
			ErrorMessage: err.Error(),
			StatusCode:   http.StatusBadRequest,
		}
		return
	}

	var lifeCycle = NewLifecycleConfiguration()
	if err = UnmarshalXMLEntity(requestBody, lifeCycle); err != nil {
		log.LogWarnf("putBucketLifecycle failed: decode request body err: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = LifeCycleErrMalformedXML
		return
	}

	ok, errorCode := lifeCycle.Validate()
	if !ok {
		log.LogErrorf("putBucketLifecycle failed: validate err: requestID(%v) lifeCycle(%v) err(%v)", GetRequestID(r), lifeCycle, errorCode)
		return
	}

	req := proto.LcConfiguration{
		VolName: param.Bucket(),
		Rules:   lifeCycle.Rules,
	}
	if err = o.mc.AdminAPI().SetBucketLifecycle(&req); err != nil {
		log.LogErrorf("putBucketLifecycle failed: SetBucketLifecycle err: bucket[%v] err(%v)", param.Bucket(), err)
		return
	}

	log.LogInfof("putBucketLifecycle success: requestID(%v) volume(%v) lifeCycle(%v)",
		GetRequestID(r), param.Bucket(), lifeCycle)
}

// API reference: https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_DeleteBucketLifecycle.html
func (o *ObjectNode) deleteBucketLifecycleConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode

	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	if _, err = o.vm.Volume(param.Bucket()); err != nil {
		errorCode = NoSuchBucket
		return
	}

	if err = o.mc.AdminAPI().DelBucketLifecycle(param.Bucket()); err != nil {
		log.LogErrorf("deleteBucketLifecycle failed: bucket[%v] err(%v)", param.Bucket(), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
