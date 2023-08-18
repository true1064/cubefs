package apinode

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/cubefs/cubefs/apinode/auth"
	"github.com/cubefs/cubefs/apinode/drive"
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type authenticator struct {
	auth auth.Auth
}

type queryItem struct {
	key string
	val string
}

type queryItemSlice []queryItem

func (s queryItemSlice) Len() int           { return len(s) }
func (s queryItemSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s queryItemSlice) Less(i, j int) bool { return s[i].key < s[j].key }

func newAuthenticator(hostport, appkey string) rpc.ProgressHandler {
	return &authenticator{
		auth: auth.NewAuth(hostport, appkey),
	}
}

func (m *authenticator) Handler(w http.ResponseWriter, req *http.Request, f func(http.ResponseWriter, *http.Request)) {
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
			errStr := fmt.Sprintf("{\"code\":\"%s\", \"message\":\"%s\"}", e.Code, e.Message)
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
	st := time.Now()
	ssoid, err = m.auth.VerifyToken(ctx, token)
	if err != nil {
		return
	}
	span.AppendTrackLog("vt", st, nil)

	if err = verifySign(sign, ssoid, req); err != nil {
		span.Errorf("verify sign error: %v", err)
		return
	}
	req.Header.Set(drive.HeaderUserID, ssoid)
	span.Debugf("set %s: %s", drive.HeaderUserID, ssoid)

	f(w, req)
}

func verifySign(sign, ssoid string, req *http.Request) error {
	var querys queryItemSlice
	values := req.URL.Query()
	for key, vals := range values {
		for _, val := range vals {
			querys = append(querys, queryItem{key, val})
		}
	}
	sort.Sort(querys)
	var queryStr string
	for i, item := range querys {
		queryStr += item.key + "=" + item.val
		if i < len(querys)-1 {
			queryStr += "&"
		}
	}
	querys = querys[:0]
	for key, vals := range req.Header {
		if !strings.HasPrefix(strings.ToLower(key), "x-cfa-") {
			continue
		}
		for _, val := range vals {
			querys = append(querys, queryItem{strings.ToLower(key), val})
		}
	}
	sort.Sort(querys)
	var headStr string
	for i, item := range querys {
		headStr += item.key + "=" + item.val
		if i < len(querys)-1 {
			headStr += "&"
		}
	}

	signStr := req.Method + "\n" + req.URL.Path + "\n" + queryStr + "\n" + headStr
	mac := hmac.New(sha1.New, []byte(ssoid))
	mac.Write([]byte(signStr))
	expectSign := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if sign != expectSign {
		log.Errorf("signStr=%s", signStr)
		return &sdk.Error{
			Status:  sdk.ErrUnauthorized.Status,
			Code:    sdk.ErrUnauthorized.Code,
			Message: fmt.Sprintf("origin sign is %s, expect %s", sign, expectSign),
		}
	}
	return nil
}
