package apinode

import (
	"fmt"
	"net/http"
	"runtime/debug"

	"github.com/cubefs/cubefs/apinode/drive"
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"golang.org/x/time/rate"
)

type limiter struct {
	limiter *rate.Limiter
}

func newLimiter(l *rate.Limiter) rpc.ProgressHandler {
	return &limiter{
		limiter: l,
	}
}

func (m *limiter) Handler(w http.ResponseWriter, req *http.Request, f func(http.ResponseWriter, *http.Request)) {
	var (
		span trace.Span
		err  *sdk.Error
	)
	ctx := req.Context()
	if rid := req.Header.Get(drive.HeaderRequestID); rid != "" {
		span, _ = trace.StartSpanFromContextWithTraceID(ctx, "", rid)
	} else {
		span = trace.SpanFromContextSafe(ctx)
	}

	defer func() {
		p := recover()
		if p != nil {
			log.Printf("WARN: panic fired in %v.panic - %v\n", f, p)
			log.Println(string(debug.Stack()))
			w.WriteHeader(597)
			return
		}

		if err == nil {
			return
		}

		w.Header().Set(trace.GetTraceIDKey(), span.TraceID())
		w.Header().Set(rpc.HeaderContentType, rpc.MIMEJSON)

		errStr := fmt.Sprintf("{\"code\":\"%s\", \"message\":\"%s\"}", err.Code, err.Message)
		w.Header().Set(rpc.HeaderContentLength, fmt.Sprint(len(errStr)))
		w.WriteHeader(err.Status)
		w.Write([]byte(errStr))
	}()

	if !m.limiter.Allow() {
		span.Error("too many requests")
		err = &sdk.Error{
			Status:  http.StatusTooManyRequests,
			Code:    "TooManyRequests",
			Message: "too many requests",
		}
		return
	}

	f(w, req)
}
