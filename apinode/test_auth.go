package apinode

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/cubefs/cubefs/apinode/drive"
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

type testAuth struct{}

func newTestAuth() rpc.ProgressHandler {
	return &testAuth{}
}

func (m *testAuth) Handler(w http.ResponseWriter, req *http.Request, f func(http.ResponseWriter, *http.Request)) {
	if isMetricRequest(req) {
		f(w, req)
		return
	}

	var (
		span trace.Span
		err  error
	)
	ctx := req.Context()
	if rid := req.Header.Get(drive.HeaderRequestID); rid != "" {
		span, _ = trace.StartSpanFromContextWithTraceID(ctx, "", rid)
	} else {
		span = trace.SpanFromContextSafe(ctx)
	}

	defer func() {
		if err == nil {
			return
		}

		w.Header().Set(trace.GetTraceIDKey(), span.TraceID())
		w.Header().Set(rpc.HeaderContentType, rpc.MIMEJSON)

		if e, ok := err.(*sdk.Error); ok {
			errStr := fmt.Sprintf("{\"code\":\"%s\", \"error\":\"%s\"}", e.Code, e.Message)
			w.Header().Set(rpc.HeaderContentLength, fmt.Sprint(len(errStr)))
			w.WriteHeader(e.Status)
			w.Write([]byte(errStr))
		} else {
			w.WriteHeader(sdk.ErrUnauthorized.Status)
		}
	}()

	authVal := req.Header.Get("Authorization")
	if !strings.HasPrefix(authVal, "cfa ") {
		span.Error("invalid head Authorization: ", authVal)
		err = sdk.ErrUnauthorized
		return
	}

	tmp := strings.SplitN(strings.TrimPrefix(authVal, "cfa "), ":", 2)
	if len(tmp) != 2 || len(tmp[0]) == 0 || len(tmp[1]) == 0 {
		span.Error("invalid head Authorization: ", authVal)
		err = sdk.ErrUnauthorized
		return
	}
	var (
		token string
		sign  string
		ssoid string
	)
	token = tmp[0]
	sign = tmp[1]
	ssoid = token

	if err = verifySign(sign, ssoid, req); err != nil {
		span.Errorf("verify sign error: %v, ssoid=%s", err, ssoid)
		return
	}
	req.Header.Set(drive.HeaderUserID, ssoid)
	span.Debugf("set %s: %s", drive.HeaderUserID, ssoid)

	f(w, req)
}
