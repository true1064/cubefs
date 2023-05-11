package cipher

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"

	"oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/cw"
	authClientType "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/auth_client/types"
	authType "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/seckit_auth/types"
	"oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/util"
)

const AUTH_PROTO_GUARD = 20190831

func Encrypt(plain []byte, dekStr string, encALg authClientType.EncAlgType) ([]byte, error) {
	ek, _ := base64.StdEncoding.DecodeString(dekStr)
	switch encALg {
	case authClientType.AES_128_GCM_NOPADDING:
		return cw.AESGCMPackEncrypt(ek[:16], plain)
	case authClientType.AES_256_GCM_NOPADDING:
		return cw.AESGCMPackEncrypt(ek, plain)
	case authClientType.AES_128_CTR_NOPADDING:
		return cw.AESCTRPackEncrypt(ek[:16], plain)
	case authClientType.AES_256_CTR_NOPADDING:
		return cw.AESCTRPackEncrypt(ek, plain)
	case authClientType.AES_128_CBC_PKCS7:
		return cw.AESCBCPackEncrypt(ek[:16], plain)
	case authClientType.AES_256_CBC_PKCS7:
		return cw.AESCBCPackEncrypt(ek, plain)

	default:
		return nil, errors.New("UnSupport algorithm: " + string(encALg))
	}
}

func Decrypt(plain []byte, dekStr string, encALg authClientType.EncAlgType) ([]byte, error) {
	ek, _ := base64.StdEncoding.DecodeString(dekStr)
	switch encALg {
	case authClientType.AES_128_GCM_NOPADDING:
		return cw.AESGCMPackDecrypt(ek[:16], plain)
	case authClientType.AES_256_GCM_NOPADDING:
		return cw.AESGCMPackDecrypt(ek, plain)
	case authClientType.AES_128_CTR_NOPADDING:
		return cw.AESCTRPackDecrypt(ek[:16], plain)
	case authClientType.AES_256_CTR_NOPADDING:
		return cw.AESCTRPackDecrypt(ek, plain)
	case authClientType.AES_128_CBC_PKCS7:
		return cw.AESCBCPackDecrypt(ek[:16], plain)
	case authClientType.AES_256_CBC_PKCS7:
		return cw.AESCBCPackDecrypt(ek, plain)

	default:
		return nil, errors.New("UnSupport algorithm: " + string(encALg))
	}
}

func GetPack(plain []byte, header *authClientType.Header, dekStr, mkStr string, secLevel int) (*authClientType.Pack, error) {
	pack := &authClientType.Pack{}
	var body string
	var signaturePlain []byte
	var s []byte
	if secLevel == int(authType.High) {
		bodyBytes, _ := Encrypt(plain, dekStr, header.EncAlg)
		body = string(bodyBytes)
	} else {
		body = base64.StdEncoding.EncodeToString(plain)
	}
	if secLevel == int(authType.High) || secLevel == int(authType.Middle) {
		headerBytes, _ := json.Marshal(header)
		signaturePlain = bytes.Join([][]byte{headerBytes, []byte(body)}, nil)
		si, _ := sign(signaturePlain, mkStr, header.SigAlg)
		s = si
	}
	pack.Body = body
	headerBytes, _ := json.Marshal(header)
	pack.Header = string(headerBytes)
	pack.AuthProtoGuard = AUTH_PROTO_GUARD
	if s != nil {
		pack.Signature = base64.StdEncoding.EncodeToString(s)
	}
	return pack, nil
}

func VerifyPack(pack *authClientType.Pack, mk string, sigAlg authClientType.SigAlgType) error {
	if pack.Signature == "" {
		return errors.New("Signature vacant")
	}
	signature, err := base64.StdEncoding.DecodeString(pack.Signature)
	headerBytes := []byte(pack.Header)
	bodyBytes := []byte(pack.Body)
	signPlain := bytes.Join([][]byte{headerBytes, bodyBytes}, nil)
	res, err := verify(signPlain, signature, mk, sigAlg)
	if err != nil || !res {
		return errors.New("Verify error")
	}
	return nil
}

func sign(signaturePlain []byte, mkStr string, sigAlg authClientType.SigAlgType) ([]byte, error) {
	mk, _ := base64.StdEncoding.DecodeString(mkStr)
	switch sigAlg {
	case authClientType.NO_SIG_ALG:
		return nil, nil
	case authClientType.HS256:
		return util.SignWithHmca(signaturePlain, mk, util.SHA_256), nil
	case authClientType.HS384:
		return util.SignWithHmca(signaturePlain, mk, util.SHA_384), nil
	case authClientType.HS512:
		return util.SignWithHmca(signaturePlain, mk, util.SHA_512), nil
	default:
		return nil, errors.New("UnSupport signature algorithm: " + string(sigAlg))
	}
}

func verify(signaturePlain, signature []byte, mkStr string, sigAlg authClientType.SigAlgType) (bool, error) {
	mk, _ := base64.StdEncoding.DecodeString(mkStr)
	switch sigAlg {
	case authClientType.HS256:
		return util.VerifyWithHmac(signaturePlain, signature, mk, util.SHA_256), nil
	case authClientType.HS384:
		return util.VerifyWithHmac(signaturePlain, signature, mk, util.SHA_384), nil
	case authClientType.HS512:
		return util.VerifyWithHmac(signaturePlain, signature, mk, util.SHA_512), nil
	default:
		return false, errors.New("UnSupport signature algorithm: " + string(sigAlg))
	}
}
