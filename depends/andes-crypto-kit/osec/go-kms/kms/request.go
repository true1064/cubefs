package kms

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	errors "github.com/pkg/errors"

	v2 "oppo.com/andes-crypto/kit/osec/seckit-go/signer/v2"
)

var confPath = "../conf/kms.conf"

// A Request is the service request to be made.
type Request struct {
	serverURL    string
	commonHeader map[string]string
	client       *http.Client
	signer       *v2.Signer
}

// KMSError ...
type KMSError struct {
	RequestID string `json:"request_id"`
	Code      string `json:"code"`
	Message   string `json:"message"`
}

func (be KMSError) Error() string {
	return fmt.Sprintf("[%s] [%s] %s", be.RequestID, be.Code, be.Message)
}

// ProtoPack ...
type ProtoPack struct {
	ProtoType    string  `json:"proto_type"`
	ProtoVersion int     `json:"proto_version"`
	RequestID    string  `json:"request_id"`
	ProtoBody    CmdPack `json:"proto_body"`
}

// CmdPack ...
type CmdPack struct {
	Action string `json:"action"`
	Params string `json:"params"`
}

func NewSigner(ak, sk string) *v2.Signer {
	return &v2.Signer{
		Credentials: v2.NewStaticCredentials(ak, sk, ""),
	}
}

// newRequest creates a new request for a KMSClient operation
func newRequest(server string, header map[string]string, signer *v2.Signer) (*Request, error) {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 1000
	http.DefaultTransport.(*http.Transport).MaxIdleConns = 1000
	//CertPool代表一个证书集合/证书池。
	//创建一个CertPool
	//pool := x509.NewCertPool()
	//flag.Parse()
	//cfg, err := goconfig.LoadConfigFile(confPath)
	//caCertPath, err := cfg.GetValue("server", "CertPath")
	//if err != nil{
	//	return nil, err
	//}
	////调用ca.crt文件
	//caCrt, err := ioutil.ReadFile(caCertPath)
	//if err != nil {
	//	fmt.Println("ReadFile err:", err)
	//	return nil, err
	//}
	////解析证书
	//pool.AppendCertsFromPEM(caCrt)
	//
	//tr := &http.Transport{
	//	////把从服务器传过来的非叶子证书，添加到中间证书的池中，使用设置的根证书和中间证书对叶子证书进行验证。
	//	TLSClientConfig: &tls.Config{RootCAs: pool},
	//}
	return &Request{serverURL: server, commonHeader: header, client: &http.Client{}, signer: signer}, nil
}

// Send will send the request, returning error if errors are encountered.
func (r *Request) Send(apiName string, input interface{}, output interface{}, header map[string]string) error {
	// make request body
	apiParmsStr, err := json.Marshal(input)
	if err != nil {
		return errors.Wrapf(ErrJsonMarshal, "json.Marshal(%v)", input)
	}
	reqProtoPackBytes, _ := makeRequestBody(apiParmsStr, apiName)

	req, err := http.NewRequest("POST", r.serverURL, strings.NewReader(string(reqProtoPackBytes)))
	if err != nil {
		err = errors.WithMessage(err, "doRequest NewRequest")
		return err
	}

	for headerKey, headerValue := range r.commonHeader {
		req.Header.Set(headerKey, headerValue)
	}

	for headerKey, headerValue := range header {
		req.Header.Set(headerKey, headerValue)
	}

	// sign http request use signer v2
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return errors.New("read http body failed")
	}
	_, err = r.signer.Sign(req, bytes.NewReader(b), "auth", "bjht-1", time.Now())
	if err != nil {
		return errors.New("sign failed")
	}

	resp, err := r.client.Do(req)
	if err != nil && resp != nil {
		err = errors.WithMessagef(err, "doRequest client.Do %v", resp.StatusCode)
		return err
	} else if err != nil {
		err = errors.WithMessage(err, "doRequest client.Do ")
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusIMUsed {

		respBody, err := ioutil.ReadAll(resp.Body)
		resError := &KMSError{}
		err = json.Unmarshal(respBody, resError)
		if err != nil {
			return ErrJsonUnmarshal
		}
		return resError
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		err = errors.WithMessage(err, "doRequest ReadAll")
		return err
	}
	io.Copy(ioutil.Discard, resp.Body)

	resAPIParams, err := parseProtoPack(respBody)
	if err != nil {
		return err
	}

	err = json.Unmarshal(resAPIParams, output)

	return err
}

// getErrorCode ...,    return response pay load
func getErrorCode(res []byte) error {
	resError := &KMSError{}
	err := json.Unmarshal(res, resError)
	if err != nil {
		return ErrJsonUnmarshal
	}
	return resError
}

func makeRequestBody(apiParams []byte, action string) ([]byte, error) {
	requestID := uuid.New().String()

	protoPack := &ProtoPack{
		ProtoType:    "auth",
		ProtoVersion: 1,
		RequestID:    requestID,
		ProtoBody:    CmdPack{Action: action, Params: string(apiParams)},
	}

	protoPackBytes, _ := json.Marshal(protoPack)

	return protoPackBytes, nil
}

func parseProtoPack(byteProtoPack []byte) ([]byte, error) {
	protoPack := &ProtoPack{}
	err := json.Unmarshal(byteProtoPack, protoPack)
	if err != nil {
		return nil, errors.New("invalid proto pack")
	}

	return []byte(protoPack.ProtoBody.Params), nil
}
