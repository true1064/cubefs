package util

import (
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"

	errors "github.com/pkg/errors"
)

// HTTPClient ...
type HTTPClient struct {
	serverURL    string
	commonHeader map[string]string
	client       *http.Client
}

// GetHTTPClient ...
func GetHTTPClient(server string, header map[string]string) *HTTPClient {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 1000
	http.DefaultTransport.(*http.Transport).MaxIdleConns = 1000
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 1000,
			IdleConnTimeout:     time.Duration(90) * time.Second,
		},
		Timeout: 20 * time.Second,
	}

	return &HTTPClient{serverURL: server, commonHeader: header, client: client}
}

// Post ...
func (c *HTTPClient) Post(reqBody []byte, header map[string]string) ([]byte, error) {
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

		err = errors.Errorf("doRequest client.Do %v body %v", resp.StatusCode, string(respBody))
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

// PostWithAPI ...
func (c *HTTPClient) PostWithAPI(apiName string, reqBody []byte, header map[string]string) ([]byte, int32, error) {
	req, err := http.NewRequest("POST", c.serverURL+"/"+strings.ToLower(apiName), strings.NewReader(string(reqBody)))
	if err != nil {
		err = errors.WithMessage(err, "doRequest NewRequest")
		return nil, 0, err
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
		return nil, 0, err
	} else if err != nil {
		err = errors.WithMessage(err, "doRequest client.Do ")
		return nil, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusIMUsed {

		respBody, err := ioutil.ReadAll(resp.Body)
		err = errors.Errorf("doRequest client.Do %v body %v", resp.StatusCode, string(respBody))
		// code, _ := getErrorCode(respBody)
		return nil, 0, err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		err = errors.WithMessage(err, "doRequest ReadAll")
		return nil, 0, err
	}

	io.Copy(ioutil.Discard, resp.Body)

	return respBody, 0, nil
}

// PostWithURL ...
func (c *HTTPClient) PostWithURL(serverURL string, apiName string, reqBody []byte, header map[string]string) ([]byte, int32, error) {
	// req, err := http.NewRequest("POST", serverURL+"/"+strings.ToLower(apiName), strings.NewReader(string(reqBody)))
	req, err := http.NewRequest("POST", serverURL+"/", strings.NewReader(string(reqBody)))
	if err != nil {
		err = errors.WithMessage(err, "doRequest NewRequest")
		return nil, 0, err
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
		return nil, 0, err
	} else if err != nil {
		err = errors.WithMessage(err, "doRequest client.Do ")
		return nil, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusIMUsed {

		respBody, err := ioutil.ReadAll(resp.Body)
		err = errors.Errorf("doRequest client.Do %v body %v", resp.StatusCode, string(respBody))
		// code, _ := getErrorCode(respBody)
		return nil, 0, err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		err = errors.WithMessage(err, "doRequest ReadAll")
		return nil, 0, err
	}

	io.Copy(ioutil.Discard, resp.Body)

	return respBody, 0, nil
}

// // getErrorCode ...,    return response pay load
// func getErrorCode(res []byte) (int32, error) {
// 	resError := &okeyproto.KMSError{}
// 	err := protobuf.Unmarshal(res, resError)
// 	if err != nil {
// 		return -1, err
// 	}

// 	return resError.Code, nil
// }
