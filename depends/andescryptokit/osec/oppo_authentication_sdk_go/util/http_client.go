package util

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	authClientType "andescryptokit/osec/oppo_authentication_sdk_go/service/auth_client/types"

	errors "github.com/pkg/errors"
)

// HttpClient ...
type HttpClient struct {
	serverURL    string
	commonHeader map[string]string
	client       *http.Client
}

type respBodyParam struct {
	RequestID string `json:"request_id"`
	Code      int    `json:"code"`
	Msg       string `json:"msg"`
}

// getHttpClient ...
func GetHttpClient(server string, header map[string]string) *HttpClient {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 1000
	http.DefaultTransport.(*http.Transport).MaxIdleConns = 1000
	return &HttpClient{serverURL: server, commonHeader: header, client: &http.Client{}}
}

// Post ...
func (c *HttpClient) Post(reqBody []byte, header map[string]string) ([]byte, error) {
	req, err := http.NewRequest("POST", c.serverURL, strings.NewReader(string(reqBody)))
	if err != nil {
		err = errors.WithMessage(err, "doRequest NewRequest")
		return nil, err
	}

	for headerKey, headerValue := range c.commonHeader {
		req.Header.Set(headerKey, headerValue)
	}

	for headerKey, headerValue := range header {
		req.Header.Set(headerKey, headerValue)
	}

	resp, err := c.client.Do(req)
	if err != nil && resp != nil {
		err = errors.WithMessagef(err, "doRequest client.Do %v", resp.StatusCode)
		return nil, err
	} else if err != nil {
		err = errors.WithMessage(err, "doRequest client.Do ")
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusIMUsed {
		respBody, err := ioutil.ReadAll(resp.Body)
		respBodyParam := &respBodyParam{}
		json.Unmarshal(respBody, respBodyParam)

		errMsg := respBodyParam.Msg
		switch respBodyParam.Code {
		case authClientType.AuthReqErrInvalidCredential:
			errMsg = authClientType.AUTH_ERR_INVALID_CREDENTIAL
		case authClientType.AuthReqErrCredentialExpired:
			errMsg = authClientType.AUTH_ERR_EXPIRED_CREDENTIAL
		case authClientType.AuthReqErrNoPermission:
			errMsg = authClientType.AUTH_ERR_NO_PERMISSION
		case authClientType.AuthReqErrBadService:
			errMsg = authClientType.AUTH_ERR_BAD_SERVICE
		default:
			errMsg = respBodyParam.Msg
		}
		fmt.Println(errors.Errorf("doRequest client.Do %v body %v errMsg %v", resp.StatusCode, string(respBody), errMsg))
		err = fmt.Errorf("RequestID: %s  error: %s", respBodyParam.RequestID, errMsg)
		return nil, err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		err = errors.WithMessage(err, "doRequest ReadAll")
		return nil, err
	}

	io.Copy(ioutil.Discard, resp.Body)

	return respBody, nil
}
