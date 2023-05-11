package kms

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"

	"github.com/pkg/errors"
)

const (
	EncAlgAES128GCM = "AES-128-GCM"
	EncAlgAES192GCM = "AES-192-GCM"
	EncAlgAES256GCM = "AES-256-GCM"

	PaddingPKCS7     = "PKCS7"
	PaddingNoPadding = "NoPadding"
	GcmTagSize       = 16
)

type CMSEncryptedData struct {
	Version       int    `json:"version"`
	Alg           string `json:"alg"`
	Padding       string `json:"padding"`
	Iv            string `json:"iv"`
	Tag           string `json:"tag"`
	EncryptedData string `json:"encrypted_data"`
}

// GenerateRand ...
func GenerateRand(bytesLen int32) ([]byte, error) {
	buf := make([]byte, bytesLen)
	_, err := rand.Read(buf)
	if err != nil {
		return nil, errors.Wrapf(ErrCryptoGenerateRandom, "rand.Read(buf): %v", err)
	}

	return buf, nil
}

// CMSEncryptJson encrypt plain to CMS cipher by json format
func AESGCMEncryptJson(plain []byte, key []byte, aad []byte, keyVersion int) ([]byte, error) {
	var err error
	var algo string

	switch len(key) {
	case 16:
		algo = EncAlgAES128GCM
	case 24:
		algo = EncAlgAES192GCM
	case 32:
		algo = EncAlgAES256GCM
	default:
		return nil, errors.Wrapf(ErrCryptoIllegalKeyLength, "key bit length(%d)", len(key)*8)
	}

	iv, err := GenerateRand(12)
	if err != nil {
		return nil, errors.WithMessage(err, "GenerateRand()")
	}

	cipher, err := AESGCMEncrypt(key, plain, iv, aad)
	if err != nil {
		return nil, errors.WithMessage(err, "AESGCMEncrypt()")
	}

	cms := &CMSEncryptedData{
		Version:       keyVersion,
		Alg:           algo,
		Padding:       PaddingNoPadding,
		Iv:            base64.StdEncoding.EncodeToString(iv),
		EncryptedData: base64.StdEncoding.EncodeToString(cipher[:len(cipher)-GcmTagSize]),
		Tag:           base64.StdEncoding.EncodeToString(cipher[len(cipher)-GcmTagSize:]),
	}

	cmsBytes, err := json.Marshal(cms)
	if err != nil {
		err = errors.Wrapf(ErrCryptoMarshalCMS, "json.Marshal(cms): %v", err)
		return nil, err
	}

	return cmsBytes, nil
}

// CMSDecryptJson decrypt CMS cipher by json format
func AESGCMDecryptJson(cmsBytes []byte, key []byte, aad []byte) ([]byte, error) {
	var plain []byte
	var err error

	if cmsBytes == nil || key == nil {
		return nil, errors.Wrapf(ErrCryptoParamsInvalid, "cmdBytes len(%v), key len(%v)", len(cmsBytes), len(key))
	}
	cms := &CMSEncryptedData{}
	err = json.Unmarshal(cmsBytes, cms)
	if err != nil {
		err = errors.Wrapf(ErrCryptoUnmarshalCMS, "json.Unmarshal(cmsBytes, cms): %v", err)
		return nil, err
	}

	if cms.Padding != PaddingPKCS7 && cms.Padding != PaddingNoPadding {
		err = errors.Wrapf(ErrCryptoUnmarshalCMS, "CMS Padding(%s)", cms.Padding)
		return nil, err
	}

	EncryptedDataBytes, err := base64.StdEncoding.DecodeString(cms.EncryptedData)
	if err != nil {
		err = errors.Wrapf(ErrCryptoUnmarshalCMS, "base64.StdEncoding.DecodeString(cms.EncryptedData): %v %v", cms.EncryptedData, err)
		return nil, err
	}
	tagBytes, err := base64.StdEncoding.DecodeString(cms.Tag)
	if err != nil {
		err = errors.Wrapf(ErrCryptoUnmarshalCMS, "base64.StdEncoding.DecodeString(cms.Tag): %v %v", cms.Tag, err)
		return nil, err
	}
	ivBytes, err := base64.StdEncoding.DecodeString(cms.Iv)
	if err != nil {
		err = errors.Wrapf(ErrCryptoUnmarshalCMS, "base64.StdEncoding.DecodeString(cms.Iv): %v %v", cms.Iv, err)
		return nil, err
	}
	aadBytes := aad

	cipher := append(EncryptedDataBytes, tagBytes...)

	switch cms.Alg {
	case EncAlgAES128GCM:
		plain, err = AESGCMDecrypt(key[:16], cipher, ivBytes, aadBytes)
	case EncAlgAES192GCM:
		plain, err = AESGCMDecrypt(key[:24], cipher, ivBytes, aadBytes)
	case EncAlgAES256GCM:
		plain, err = AESGCMDecrypt(key[:32], cipher, ivBytes, aadBytes)
	default:
		err = errors.Wrapf(ErrCryptoUnmarshalCMS, "CMS Alg(%v)", cms.Alg)
		return nil, err
	}

	if err != nil {
		err = errors.WithMessagef(err, "AESGCMDecrypt failed with Alg(%v)", cms.Alg)
	}

	return plain, err
}

// AESGCMEncrypt
func AESGCMEncrypt(key []byte, plain []byte, nonce []byte, aad []byte) (encrypted []byte, err error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.Wrapf(ErrCryptoAESGCMEncrypt, "aes.NewCipher(key): %v", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.Wrapf(ErrCryptoAESGCMEncrypt, "cipher.NewGCM(block): %v", err)
	}

	encrypted = gcm.Seal(nil, nonce, plain, aad)

	return encrypted, nil
}

// AESGCMDecrypt
func AESGCMDecrypt(key []byte, encrypted []byte, nonce []byte, aad []byte) (plain []byte, err error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.Wrapf(ErrCryptoAESGCMDecrypt, "aes.NewCipher(key): %v", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.Wrapf(ErrCryptoAESGCMDecrypt, "cipher.NewGCM(block): %v", err)
	}

	plain, err = gcm.Open(nil, nonce, encrypted, aad)
	if err != nil {
		return nil, errors.Wrapf(ErrCryptoAESGCMDecrypt, "gcm.Open(): %v", err)
	}
	return plain, nil
}
