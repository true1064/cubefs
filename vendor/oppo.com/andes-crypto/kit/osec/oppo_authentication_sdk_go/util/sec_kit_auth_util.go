package util

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"hash"

	authType "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/seckit_auth/types"

	"golang.org/x/crypto/pbkdf2"
)

const (
	SHA_256 = 1
	SHA_384 = 2
	SHA_512 = 3
)

// 获取派生SessionKey
func GenerateKey(ak, sk string) (*authType.SessionKey, error) {
	salt := []byte(ak + "@oppo.com:auth")
	data, err := hex.DecodeString(sk)
	if err != nil {
		return nil, err
	}
	dk := pbkdf2.Key(data, salt, 10000, 64, sha256.New)
	mk := make([]byte, 32)
	dek := make([]byte, 32)
	mk = dk[:32]
	dek = dk[32:]

	sessionKey := &authType.SessionKey{
		MK: base64.StdEncoding.EncodeToString(mk),
		EK: base64.StdEncoding.EncodeToString(dek),
	}
	return sessionKey, nil
}

func GenerateRand(bitLength int) (rn []byte, err error) {
	if bitLength%8 != 0 {
		err = errors.New("GenerateRand: bitLength should multiple of 8 bits.")
		return nil, err
	}

	rn = make([]byte, bitLength/8)
	_, err = rand.Read(rn)
	if nil != err {
		err = errors.New("GenerateRand")
		return nil, err
	}

	return rn, err
}

func GetUuid() (string, error) {
	rn, err := GenerateRand(32)
	if nil != err {
		err = errors.New("GenerateRand")
		return "", err
	}

	return hex.EncodeToString(rn), nil
}

func SignWithHmca(signPlain, mkStr []byte, h int) []byte {
	var hFunc func() hash.Hash
	switch h {
	case SHA_256:
		hFunc = sha256.New
	case SHA_384:
		hFunc = sha512.New384
	case SHA_512:
		hFunc = sha512.New
	default:
		hFunc = sha256.New
	}
	hmacFunc := hmac.New(hFunc, mkStr)
	hmacFunc.Write(signPlain)
	return hmacFunc.Sum(nil)
}

func VerifyWithHmac(signPlain, sign, mk []byte, hash int) bool {
	sig := SignWithHmca(signPlain, mk, hash)
	return bytes.Equal(sig, sign)
}
