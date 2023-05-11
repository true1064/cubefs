package okey

import (
	"encoding/base64"
	"encoding/json"

	"github.com/pkg/errors"
	oppoauthkit "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/seckit_auth"
	oppoauthcipher "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/seckit_auth/cipher"

	kmserr "oppo.com/andes-crypto/kit/osec/seckit-go/okey/error"
	"oppo.com/andes-crypto/kit/osec/seckit-go/okey/model"
	"oppo.com/andes-crypto/kit/osec/seckit-go/okey/util"
)

type KmsTransmission struct {
	KmsName       string // default value: okey
	AppName       string //
	Ak            string
	Sk            string
	KmsUrl        string
	AuthUrl       string
	SecKitAuth    *oppoauthkit.SecKitAuth
	CipherManager *oppoauthcipher.SecKitAuthClientCipherManager
}

// var KmsTransObj *KmsTransmission = &KmsTransmission{}

// GetKMSTransObject ...
func GetKMSTransObject(appName, ak, sk, kmsUrl, authUrl string) *KmsTransmission {
	// KmsTransObj.KmsName = "okey"
	// KmsTransObj.AppName = appName
	// KmsTransObj.Ak = ak
	// KmsTransObj.Sk = sk
	// KmsTransObj.KmsUrl = kmsUrl
	// KmsTransObj.AuthUrl = authUrl
	KmsTransObj := &KmsTransmission{
		KmsName: "okey",
		AppName: appName,
		Ak:      ak,
		Sk:      sk,
		KmsUrl:  kmsUrl,
		AuthUrl: authUrl,
	}

	return KmsTransObj
}

// Init ... initialize, get appname's kek and sessionkey witch is used for communicating with okey server
func (transObj *KmsTransmission) Init() error {
	var err error
	transObj.SecKitAuth, err = oppoauthkit.GetInstance(transObj.AppName, transObj.AuthUrl, transObj.Ak, transObj.Sk)
	if err != nil {
		return errors.WithMessagef(kmserr.ErrAuthInit, "%v", err)
	}
	transObj.CipherManager, err = transObj.SecKitAuth.GetClientCipherManager(transObj.KmsName)
	if err != nil {
		return errors.WithMessagef(kmserr.ErrAuthInit, "%v", err)
	}

	return nil
}

// CheckParam ...
func (transObj *KmsTransmission) CheckParam() error {
	if transObj == nil {
		return errors.WithMessage(kmserr.ErrParasInvalid, "kmstransobj null")
	}

	if transObj.AppName == "" {
		return errors.WithMessage(kmserr.ErrParasInvalid, "App name is null")
	}

	if transObj.Ak == "" {
		return errors.WithMessage(kmserr.ErrParasInvalid, "Ak is null")
	}

	if transObj.Sk == "" {
		return errors.WithMessage(kmserr.ErrParasInvalid, "Sk is null")
	}

	if transObj.KmsUrl == "" {
		return errors.WithMessage(kmserr.ErrParasInvalid, "kmsUrl is null")
	}

	if transObj.AuthUrl == "" {
		return errors.WithMessage(kmserr.ErrParasInvalid, "authUrl is null")
	}

	// check sk is base64 string
	//

	return nil
}

// GetKEK ...
func (kmsobj *KmsTransmission) GetKEK(targetAppName string) ([]byte, error) {
	reqGetKEK := &model.ReqGetKEK{
		AppName:       kmsobj.AppName,
		TargetAppName: targetAppName,
	}

	reqGetKEKJson, _ := json.Marshal(reqGetKEK)

	respParams, err := kmsobj.RequestKmsService(GET_KEK, reqGetKEKJson)
	if err != nil {
		return nil, err
	}

	resGetKEK := &model.ResGetKEK{}
	err = json.Unmarshal(respParams, &resGetKEK)
	if err != nil {
		return nil, errors.WithMessage(kmserr.ErrJsonFormat, "unmarshal ResGetKEK failed")
	}

	kekBytes, err := base64.StdEncoding.DecodeString(resGetKEK.Kek)
	if err != nil {
		return nil, errors.WithMessage(kmserr.ErrJsonFormat, "base64 decode failed")
	}
	// fmt.Printf("appname(%v), kek: (%v)\n", targetAppName, kekBytes)
	return kekBytes, nil
}

// RequestKmsService ...
func (transObj *KmsTransmission) RequestKmsService(action string, param []byte) ([]byte, error) {
	// get cipher object
	cipher := transObj.CipherManager.GetCipher()
	cmdPackJson := &model.CmdPack{
		Action: action,
		Param:  string(param),
	}
	cmdPackBytes, _ := json.Marshal(cmdPackJson)
	cmdPackCipher, _ := cipher.Seal(cmdPackBytes, 1, 1)

	// pack ProtoPack
	protoPackJson := &model.ProtoPack{AuthType: "auth", AuthVersion: 1, RequestID: "1", Payload: string(cmdPackCipher)}
	protoPackBytes, _ := json.Marshal(protoPackJson)

	// send and recevice
	url := transObj.KmsUrl + "/v2"
	httpClient := util.GetHTTPClient(url, map[string]string{"Content-Type": "text/plain"})
	resByte, _, err := httpClient.PostWithURL(url, action, protoPackBytes, nil)
	if err != nil {
		return nil, errors.WithMessagef(kmserr.ErrClientPost, "PostWithURL, %v", err)
	}

	// unmarshal response's protopack
	respProtoBackJson := &model.ProtoPack{}
	err = json.Unmarshal(resByte, &respProtoBackJson)
	if err != nil {
		return nil, errors.WithMessage(kmserr.ErrJsonFormat, "unmarshal protopack")
	}
	// decrypt auth cipher
	respCmdPackBytes, err := cipher.UnSeal([]byte(respProtoBackJson.Payload))
	if err != nil {
		return nil, err
	}

	respCmdPackJson := &model.CmdPack{}
	err = json.Unmarshal(respCmdPackBytes, &respCmdPackJson)
	if err != nil {
		return nil, errors.WithMessage(kmserr.ErrJsonFormat, "unmarshal cmdpack")
	}

	return []byte(respCmdPackJson.Param), nil
}

// GetSecurityPolicy ...
func (kto *KmsTransmission) GetSecurityPolicy(targetAppName string) (*model.ResGetSecurityPolicy, error) {
	req := &model.ReqGetSecurityPolicy{
		AppName:       kto.AppName,
		TargetAppName: targetAppName,
	}
	reqBytes, _ := json.Marshal(req)
	respBytes, err := kto.RequestKmsService(GET_SECURITY_POLICY, reqBytes)
	if err != nil {
		return nil, err
	}

	respJson := &model.ResGetSecurityPolicy{}
	err = json.Unmarshal(respBytes, respJson)
	if err != nil {
		return nil, nil
	}

	return respJson, nil
}

// DownloadWbKeys ...
func (kto *KmsTransmission) DownloadWbKeys(targetAppName string, lastAccessTime uint64) (*model.ResDownloadWBKeys, error) {
	req := &model.ReqDownloadWBKeys{
		AppName:        kto.AppName,
		TargetAppName:  targetAppName,
		LastAccessTime: lastAccessTime,
	}
	reqBytes, _ := json.Marshal(req)
	respBytes, err := kto.RequestKmsService(DOWNLOAD_WB_KEYS, reqBytes)
	if err != nil {
		return nil, err
	}

	respJson := &model.ResDownloadWBKeys{}
	err = json.Unmarshal(respBytes, respJson)
	if err != nil {
		return nil, nil
	}

	return respJson, nil
}

// DownloadKeyPair ...
func (kto *KmsTransmission) DownloadKeyPair(targetAppNames []string) (*model.ResDownloadKeyPair, error) {
	req := &model.ReqDownloadKeyPair{
		TargetAppnames: targetAppNames,
	}
	reqBytes, _ := json.Marshal(req)
	respBytes, err := kto.RequestKmsService(DOWNLOAD_KEY_PAIR, reqBytes)
	if err != nil {
		return nil, err
	}

	respJson := &model.ResDownloadKeyPair{}
	err = json.Unmarshal(respBytes, respJson)
	if err != nil {
		return nil, nil
	}

	return respJson, nil
}

// GetGrantedAppNames ...
func (kto *KmsTransmission) GetGrantedAppNames() ([]string, error) {
	respBytes, err := kto.RequestKmsService(GET_GRANTED_APP_NAME, []byte("{}"))
	if err != nil {
		return nil, err
	}

	respJson := &model.ResGetGrantedAppName{}
	err = json.Unmarshal(respBytes, respJson)
	if err != nil {
		return nil, nil
	}

	return respJson.TargetAppNames, nil
}

// GetGrantedAppInfo ...
func (kto *KmsTransmission) GetGrantedAppInfo(appNames []string) (*model.ResGetGrantedAppInfo, error) {
	req := &model.ReqGetGrantedAppInfo{
		TargetAppNames: appNames,
	}
	reqBytes, _ := json.Marshal(req)
	respBytes, err := kto.RequestKmsService(GET_GRANTED_APP_INFO, reqBytes)
	if err != nil {
		return nil, err
	}

	respJson := &model.ResGetGrantedAppInfo{}
	err = json.Unmarshal(respBytes, respJson)
	if err != nil {
		return nil, nil
	}

	return respJson, nil
}
