package auth

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

type Auth interface {
	VerifyToken(ctx context.Context, token string) (string, error)
}

type verifyTokenArgs struct {
	SubToken  string `json:"subToken"`
	SubAppKey string `json:"subAppKey"`
	Timestamp int64  `json:"timestamp"`
	Sign      string `json:"sign"`
}

type respData struct {
	Ssoid string `json:"ssoid"`
}

type verifyTokenResponse struct {
	Code   int      `json:"code"`
	ErrMsg string   `json:"errmsg"`
	Data   respData `json:"data"`
}

type auth struct {
	url    string
	appKey string
	client *http.Client
}

func NewAuth(hostport, appkey string) Auth {
	return &auth{
		url:    fmt.Sprintf("https://%s/sub/token/v1/auth", hostport),
		appKey: appkey,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (s *auth) VerifyToken(ctx context.Context, token string) (string, error) {
	span := trace.SpanFromContextSafe(ctx)
	args := &verifyTokenArgs{
		SubToken:  token,
		SubAppKey: s.appKey,
		Timestamp: time.Now().UnixMilli(),
	}
	signStr := fmt.Sprintf("subToken=%s&timestamp=%d&subAppKey=%s", args.SubToken, args.Timestamp, args.SubAppKey)
	sum := md5.Sum([]byte(signStr))
	args.Sign = hex.EncodeToString(sum[:])

	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("json marshal error: %v, origin str is %s", err, signStr)
		return "", &sdk.Error{
			Status:  sdk.ErrTokenVerify.Status,
			Code:    sdk.ErrTokenVerify.Code,
			Message: "marshal token request body error",
		}
	}
	req, err := http.NewRequest(http.MethodPost, s.url, bytes.NewReader(data))
	if err != nil {
		span.Errorf("new request error: %v, origin str is %s", err, signStr)
		return "", &sdk.Error{
			Status:  sdk.ErrTokenVerify.Status,
			Code:    sdk.ErrTokenVerify.Code,
			Message: "new http request error",
		}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Length", strconv.FormatInt(int64(len(data)), 10))
	resp, err := s.client.Do(req)
	if err != nil {
		span.Errorf("http post error: %v, origin str is %s", err, signStr)
		return "", &sdk.Error{
			Status:  sdk.ErrTokenVerify.Status,
			Code:    sdk.ErrTokenVerify.Code,
			Message: "remote invoke error",
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		span.Errorf("verify token return %d, origin str is %s", resp.StatusCode, signStr)
		return "", &sdk.Error{
			Status:  sdk.ErrTokenVerify.Status,
			Code:    sdk.ErrTokenVerify.Code,
			Message: fmt.Sprintf("verify token return %d", resp.StatusCode),
		}
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil || len(body) == 0 {
		var errStr string
		if err != nil {
			errStr = fmt.Sprintf("read body error: %v", err)
		} else {
			errStr = "read body error: empty body"
		}
		span.Error(errStr, "origin str is ", signStr)
		return "", &sdk.Error{
			Status:  sdk.ErrTokenVerify.Status,
			Code:    sdk.ErrTokenVerify.Code,
			Message: "verify token error",
		}
	}
	res := &verifyTokenResponse{}
	if err = json.Unmarshal(body, res); err != nil {
		span.Errorf("unmarshal resp body error: %v, body: %s, origin str is %s", err, string(body), signStr)
		return "", &sdk.Error{
			Status:  sdk.ErrTokenVerify.Status,
			Code:    sdk.ErrTokenVerify.Code,
			Message: err.Error(),
		}
	}
	switch res.Code {
	case 200:
		if len(res.Data.Ssoid) == 0 {
			span.Errorf("recv response: %s", string(body))
			err = &sdk.Error{
				Status:  sdk.ErrTokenVerify.Status,
				Code:    sdk.ErrTokenVerify.Code,
				Message: "ssoid is empty",
			}
		}
	case 4041:
		err = sdk.ErrTokenExpires
	case 4042:
		err = sdk.ErrAppExit
	case 4043:
		err = sdk.ErrAccExit
	default:
		err = &sdk.Error{
			Status:  sdk.ErrTokenVerify.Status,
			Code:    sdk.ErrTokenVerify.Code,
			Message: fmt.Sprintf("{code: %d, errmsg: %s}", res.Code, res.ErrMsg),
		}
	}
	if err != nil {
		span.Errorf("verify token error: %v, origin str is %s", err, signStr)
		return "", err
	}
	return res.Data.Ssoid, nil
}
