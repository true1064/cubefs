package apinode

import (
	"encoding/json"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/apinode/drive"
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/rpc/auditlog"
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
		st   = time.Now()
		span trace.Span
		err  *sdk.Error
	)
	ctx := req.Context()
	req = req.WithContext(auditlog.ContextWithStartTime(ctx, st))
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
		replyWithError(w, err)
	}()

	if !m.limiter.Allow() {
		span.Error("too many requests")
		err = sdk.ErrLimitExceed
		return
	}

	f(w, req)
	span.Debug("request spent", time.Since(st))
}

type errorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code,omitempty"`
}

func replyWithError(w http.ResponseWriter, err error) {
	httpErr := rpc.Error2HTTPError(err)
	data, e := json.Marshal(errorResponse{
		Error: httpErr.Error(),
		Code:  httpErr.ErrorCode(),
	})
	if e != nil {
		w.WriteHeader(httpErr.StatusCode())
		return
	}

	h := w.Header()
	h.Set(rpc.HeaderContentLength, strconv.Itoa(len(data)))
	h.Set(rpc.HeaderContentType, rpc.MIMEJSON)
	w.WriteHeader(httpErr.StatusCode())
	w.Write(data)
}
