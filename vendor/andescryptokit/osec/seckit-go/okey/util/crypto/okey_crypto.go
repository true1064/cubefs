package okeycrypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"hash"
	"reflect"
	"strings"

	"github.com/pkg/errors"

	// secsdk "gitlab.os.adc.com/osec/crypto/crypto_go/cgo"
	"andescryptokit/osec/seckit-go/cw"
	jsonmodel "andescryptokit/osec/seckit-go/okey/model"

	// pb "gitlab.os.adc.com/osec/okey/proto"
	"golang.org/x/crypto/pbkdf2"
)

const (
	EncAlgAES128GCM = "AES-128-GCM"
	EncAlgAES192GCM = "AES-192-GCM"
	EncAlgAES256GCM = "AES-256-GCM"

	PaddingPKCS7     = "PKCS7"
	PaddingNoPadding = "NoPadding"

	SignAlgHMAC = "HMAC"

	HashIDSHA256 = "SHA256"
)

// // CMSEncryptPB encrypt plain to CMS format cipher
// func CMSEncryptPB(plain []byte, key []byte) ([]byte, error) {
// 	var err error

// 	if len(key) != 32 {
// 		return nil, errors.Wrapf(ErrCryptoIllegalKeyLength, "key bit length(%d)", len(key)*8)
// 	}

// 	iv, err := GenerateRand(12)
// 	if err != nil {
// 		return nil, errors.WithMessage(err, "GenerateRand()")
// 	}

// 	cipher, err := AESGCMEncrypt(key, plain, iv, nil)
// 	if err != nil {
// 		return nil, errors.WithMessage(err, "AESGCMEncrypt()")
// 	}

// 	cms := &pb.CMSEncryptedData{
// 		Alg:           EncAlgAES256GCM,
// 		Padding:       PaddingNoPadding,
// 		Iv:            iv,
// 		EncryptedData: cipher[:len(cipher)-16],
// 		Tag:           cipher[len(cipher)-16:],
// 	}

// 	cmsBytes, err := protobuf.Marshal(cms)
// 	if err != nil {
// 		err = errors.Wrapf(ErrCryptoPBMarshalCMS, "protobuf.Marshal(cms): %v", err)
// 		return nil, err
// 	}

// 	return cmsBytes, nil
// }

// CMSEncryptJson encrypt plain to CMS cipher by json format
func CMSEncryptJson(plain []byte, key []byte, aad []byte) ([]byte, error) {
	var err error

	if len(key) != 32 {
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

	cms := &jsonmodel.CMSEncryptedData{
		Alg:           EncAlgAES256GCM,
		Padding:       PaddingNoPadding,
		Iv:            base64.StdEncoding.EncodeToString(iv),
		Aad:           base64.StdEncoding.EncodeToString(aad),
		EncryptedData: base64.StdEncoding.EncodeToString(cipher[:len(cipher)-16]),
		Tag:           base64.StdEncoding.EncodeToString(cipher[len(cipher)-16:]),
	}

	cmsBytes, err := json.Marshal(cms)
	if err != nil {
		err = errors.Wrapf(ErrCryptoPBMarshalCMS, "protobuf.Marshal(cms): %v", err)
		return nil, err
	}

	return cmsBytes, nil
}

// // CMSDecrypt decrypt CMS format cipher
// func CMSDecrypt(cmsBytes []byte, key []byte) ([]byte, error) {
// 	var plain []byte
// 	var err error

// 	if cmsBytes == nil || key == nil {
// 		return nil, errors.Wrapf(ErrCryptoParamsInvalid, "cmdBytes len(%v), key len(%v)", len(cmsBytes), len(key))
// 	}
// 	cms := &pb.CMSEncryptedData{}
// 	err = protobuf.Unmarshal(cmsBytes, cms)
// 	if err != nil {
// 		err = errors.Wrapf(ErrCryptoPBUmarshalCMS, "protobuf.Unmarshal(cmsBytes, cms): %v", err)
// 		return nil, err
// 	}

// 	if cms.Padding != PaddingPKCS7 && cms.Padding != PaddingNoPadding {
// 		err = errors.Wrapf(ErrCryptoCMSPaddingNotSupported, "CMS Padding(%s)", cms.Padding)
// 		return nil, err
// 	}

// 	cipher := append(cms.EncryptedData, cms.Tag...)

// 	switch cms.Alg {
// 	case EncAlgAES128GCM:
// 		plain, err = AESGCMDecrypt(key[:16], cipher, cms.Iv, cms.Aad)
// 	case EncAlgAES192GCM:
// 		plain, err = AESGCMDecrypt(key[:24], cipher, cms.Iv, cms.Aad)
// 	case EncAlgAES256GCM:
// 		plain, err = AESGCMDecrypt(key[:32], cipher, cms.Iv, cms.Aad)
// 	default:
// 		err = errors.Wrapf(ErrCryptoCMSEncAlgNotSupported, "CMS Alg(%v)", cms.Alg)
// 		return nil, err
// 	}

// 	if err != nil {
// 		err = errors.WithMessagef(err, "AESGCMDecrypt failed with Alg(%v)", cms.Alg)
// 	}

// 	return plain, err
// }

// CMSDecryptJson decrypt CMS cipher by json format
func CMSDecryptJson(cmsBytes []byte, key []byte) ([]byte, error) {
	var plain []byte
	var err error

	if cmsBytes == nil || key == nil {
		return nil, errors.Wrapf(ErrCryptoParamsInvalid, "cmdBytes len(%v), key len(%v)", len(cmsBytes), len(key))
	}
	cms := &jsonmodel.CMSEncryptedData{}
	err = json.Unmarshal(cmsBytes, cms)
	if err != nil {
		err = errors.Wrapf(ErrCryptoPBUmarshalCMS, "json.Unmarshal(cmsBytes, cms): %v", err)
		return nil, err
	}

	if cms.Padding != PaddingPKCS7 && cms.Padding != PaddingNoPadding {
		err = errors.Wrapf(ErrCryptoCMSPaddingNotSupported, "CMS Padding(%s)", cms.Padding)
		return nil, err
	}

	EncryptedDataBytes, err := base64.StdEncoding.DecodeString(cms.EncryptedData)
	if err != nil {
		err = errors.Wrapf(ErrCryptoPBUmarshalCMS, "base64.StdEncoding.DecodeString(cms.EncryptedData): %v %v", cms.EncryptedData, err)
		return nil, err
	}
	tagBytes, err := base64.StdEncoding.DecodeString(cms.Tag)
	if err != nil {
		err = errors.Wrapf(ErrCryptoPBUmarshalCMS, "base64.StdEncoding.DecodeString(cms.Tag): %v %v", cms.Tag, err)
		return nil, err
	}
	ivBytes, err := base64.StdEncoding.DecodeString(cms.Iv)
	if err != nil {
		err = errors.Wrapf(ErrCryptoPBUmarshalCMS, "base64.StdEncoding.DecodeString(cms.Iv): %v %v", cms.Iv, err)
		return nil, err
	}
	aadBytes, err := base64.StdEncoding.DecodeString(cms.Aad)
	if err != nil {
		err = errors.Wrapf(ErrCryptoPBUmarshalCMS, "base64.StdEncoding.DecodeString(cms.Iv): %v %v", cms.Iv, err)
		return nil, err
	}
	cipher := append(EncryptedDataBytes, tagBytes...)

	switch cms.Alg {
	case EncAlgAES128GCM:
		plain, err = AESGCMDecrypt(key[:16], cipher, ivBytes, aadBytes)
	case EncAlgAES192GCM:
		plain, err = AESGCMDecrypt(key[:24], cipher, ivBytes, aadBytes)
	case EncAlgAES256GCM:
		plain, err = AESGCMDecrypt(key[:32], cipher, ivBytes, aadBytes)
	default:
		err = errors.Wrapf(ErrCryptoCMSEncAlgNotSupported, "CMS Alg(%v)", cms.Alg)
		return nil, err
	}

	if err != nil {
		err = errors.WithMessagef(err, "AESGCMDecrypt failed with Alg(%v)", cms.Alg)
	}

	return plain, err
}

// AESGCMEncrypt AESGCMEncrypt
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

// AESGCMDecrypt AESGCMDecrypt
func AESGCMDecrypt(key []byte, encrypted []byte, nonce []byte, aad []byte) (plain []byte, err error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.Wrapf(ErrCryptoAESGCMDecrypt, "aes.NewCipher(key): %v", err)
	}

	if len(nonce) == 12 {
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			return nil, errors.Wrapf(ErrCryptoAESGCMDecrypt, "cipher.NewGCM(block): %v", err)
		}

		plain, err = gcm.Open(nil, nonce, encrypted, aad)
		if err != nil {
			return nil, errors.Wrapf(ErrCryptoAESGCMDecrypt, "gcm.Open(): %v", err)
		}
	} else {
		gcm, err := cipher.NewGCMWithNonceSize(block, len(nonce))
		if err != nil {
			return nil, errors.Wrapf(ErrCryptoAESGCMDecrypt, "cipher.NewGCM(block): %v", err)
		}

		plain, err = gcm.Open(nil, nonce, encrypted, aad)
		if err != nil {
			return nil, errors.Wrapf(ErrCryptoAESGCMDecrypt, "gcm.Open(): %v", err)
		}
	}

	return plain, nil
}

// DeriveKey ...
func DeriveKey(account string, password []byte) (mk []byte, dek []byte) {
	salt := []byte(account + "@oppo.com:kms")
	deriveResult := pbkdf2.Key(password, salt, 10000, 64, sha256.New)

	mk = deriveResult[:32]
	dek = deriveResult[32:]

	return mk, dek
}

// // CMSSignPB sign messsage to CMS format signature
// func CMSSignPB(key []byte, message ...[]byte) ([]byte, error) {
// 	if len(key) != 32 {
// 		return nil, errors.Wrapf(ErrCryptoIllegalKeyLength, "key bit length(%d)", len(key)*8)
// 	}

// 	sign := HMAC(key, sha256.New, message...)

// 	cms := &pb.CMSSignedData{
// 		SignAlg:       SignAlgHMAC,
// 		HashId:        HashIDSHA256,
// 		SignedContent: sign,
// 	}

// 	cmsBytes, err := protobuf.Marshal(cms)
// 	if err != nil {
// 		err = errors.Wrapf(ErrCryptoPBMarshalCMS, "protobuf.Marshal(cms): %v", err)
// 		return nil, err
// 	}

// 	return cmsBytes, nil
// }

// CMSSignJson sign messsage to CMS format signature
func CMSSignJson(key []byte, message ...[]byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, errors.Wrapf(ErrCryptoIllegalKeyLength, "key bit length(%d)", len(key)*8)
	}

	sign := HMAC(key, sha256.New, message...)

	cms := &jsonmodel.CMSSignedData{
		SignAlg:       SignAlgHMAC,
		HashId:        HashIDSHA256,
		SignedContent: base64.StdEncoding.EncodeToString(sign),
	}

	cmsBytes, err := json.Marshal(cms)
	if err != nil {
		err = errors.Wrapf(ErrCryptoPBMarshalCMS, "protobuf.Marshal(cms): %v", err)
		return nil, err
	}

	return cmsBytes, nil
}

// // CMSVerifyPB verify CMS format signature
// func CMSVerifyPB(cmsBytes []byte, key []byte, message ...[]byte) error {
// 	var sign []byte
// 	var err error
// 	var h func() hash.Hash

// 	cms := &pb.CMSSignedData{}
// 	err = protobuf.Unmarshal(cmsBytes, cms)
// 	if err != nil {
// 		err = errors.Wrapf(ErrCryptoPBUmarshalCMS, "protobuf.Unmarshal(cmsBytes, cms): %v", err)
// 		return err
// 	}

// 	switch cms.HashId {
// 	case HashIDSHA256:
// 		h = sha256.New
// 	default:
// 		err = errors.Wrapf(ErrCryptoCMSHashIDNotSupported, "HashID(%s)", cms.HashId)
// 		return err
// 	}

// 	switch cms.SignAlg {
// 	case SignAlgHMAC:
// 		sign = HMAC(key[:32], h, message...)
// 	default:
// 		err = errors.Wrapf(ErrCryptoCMSSignAlgNotSupported, "CMS SignAlg(%s)", cms.SignAlg)
// 		return err
// 	}

// 	if 0 != bytes.Compare(cms.SignedContent, sign) {
// 		err = errors.WithStack(ErrCryptoCMSVerify)
// 		return err
// 	}

// 	return nil
// }

// CMSVerifyJson verify CMS format signature
func CMSVerifyJson(cmsBytes []byte, key []byte, message ...[]byte) error {
	var sign []byte
	var err error
	var h func() hash.Hash

	cms := &jsonmodel.CMSSignedData{}
	// fmt.Println(string(cmsBytes))
	err = json.Unmarshal(cmsBytes, cms)
	if err != nil {
		err = errors.Wrapf(ErrCryptoPBUmarshalCMS, "json.Unmarshal(cmsBytes, cms): %v", err)
		return err
	}

	switch cms.HashId {
	case HashIDSHA256:
		h = sha256.New
	default:
		err = errors.Wrapf(ErrCryptoCMSHashIDNotSupported, "HashID(%s)", cms.HashId)
		return err
	}

	switch cms.SignAlg {
	case SignAlgHMAC:
		sign = HMAC(key[:32], h, message...)
	default:
		err = errors.Wrapf(ErrCryptoCMSSignAlgNotSupported, "CMS SignAlg(%s)", cms.SignAlg)
		return err
	}

	if 0 != strings.Compare(cms.SignedContent, base64.StdEncoding.EncodeToString(sign)) {
		err = errors.WithStack(ErrCryptoCMSVerify)
		return err
	}

	return nil
}

// HMAC hash-based message authentication code
func HMAC(key []byte, h func() hash.Hash, message ...[]byte) []byte {
	md := hmac.New(h, key)
	for _, v := range message {
		md.Write(v)
	}

	return md.Sum(nil)
}

// VerifyWithHmacSHA256 ...
func VerifyWithHmacSHA256(plainText []byte, signature []byte, mk []byte) (bool, error) {
	sign := HMAC(mk, sha256.New, plainText)

	verified := bytes.Equal(signature, sign)
	return verified, nil
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

// // ECIESEnvelopeDecryptEx ...
// func ECIESEnvelopeDecryptEx(certin, sk, cipher []byte) (plain []byte, err error) {
// 	plaintext, err := secsdk.ECIESEnvelopeDecrypt_ex(certin, sk, cipher)
// 	if err != nil {

// 		osecbe, ok := err.(*be.BusinessError)
// 		if ok {
// 			switch osecbe.Code {
// 			case be.ErrOsecSDKInputInvalid.Code:
// 				return nil, errors.Wrapf(ErrCryptoEnvelopDecrptFailed, "secsdk.ECIESEnvelopeDecrypt_ex")
// 			case be.ErrOsecSDKCMSFormatInvalid.Code:
// 				return nil, errors.Wrapf(ErrCryptoEnvelopDecrptFailedCipherFormat, "secsdk.ECIESEnvelopeDecrypt_ex")
// 			case be.ErrOsecSDKCertInvalid.Code:
// 				return nil, errors.Wrapf(ErrCryptoEnvelopDecrptFailedsCertInvalid, "secsdk.ECIESEnvelopeDecrypt_ex")
// 			default:
// 				return nil, errors.Wrapf(ErrCryptoEnvelopDecrptFailed, "secsdk.ECIESEnvelopeDecrypt_ex")

// 			}
// 		}

// 		return nil, ErrCryptoNativeError
// 	}

// 	return plaintext, nil
// }

// RSAParsePubPEM parse a PKCS8 or PKCS1 encode PEM public key into *rsa.PublicKey
func RSAParsePubPEM(pemPub []byte) (pub *rsa.PublicKey, err error) {
	block, _ := pem.Decode(pemPub)
	if block == nil {
		err = errors.Errorf("decode pem public key failed.")
		return nil, err
	}

	if block.Type == "PUBLIC KEY" {

		re, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			err = errors.Wrap(err, "ParsePKIXPublicKey block.Bytes")
			return nil, err
		}

		repub, ok := re.(*rsa.PublicKey)
		if !ok {
			err = errors.Errorf("not supported kind:%v", reflect.ValueOf(re).Kind())
			return nil, err
		}

		return repub, nil

	} else if block.Type == "RSA PUBLIC KEY" {

		pub, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			err = errors.Wrap(err, "ParsePKIXPublicKey block.Bytes")
			return nil, err
		}
		repub, ok := pub.(*rsa.PublicKey)
		if !ok {
			err = errors.Errorf("not supported kind:%v", reflect.ValueOf(pub).Kind())
			return nil, err
		}
		return repub, nil
	}

	err = errors.Errorf("not support block type:%v", block.Type)
	return nil, err
}

// RSAEncryptPKCS1v15 RSA Encrypt PKCS1 v15
func RSAEncryptPKCS1v15(PEMPub []byte, data []byte) (encrypted []byte, err error) {
	pub, err := RSAParsePubPEM(PEMPub)
	if err != nil {
		return nil, err
	}

	if len(data) >= (pub.Size() - 2*sha256.Size - 2) {
		err = errors.Errorf("data too large, len:%v", len(data))
		return nil, err
	}

	return rsa.EncryptPKCS1v15(rand.Reader, pub, data)
}

// RSADecryptPKCS1OAEP decrypt oaep
func RSADecryptPKCS1OAEP(hashCtx hash.Hash, PEMPriv []byte, ciphertext []byte) (plaintext []byte, err error) {
	priv, err := cw.ParsePemRsaPrivateKey(PEMPriv)
	if err != nil {
		err = errors.Wrap(err, "ParsePemRsaPrivateKey")
		return nil, err
	}

	// return rsa.DecryptOAEP(sha256.New(), rand.Reader, priv, ciphertext, nil)
	return rsa.DecryptOAEP(hashCtx, rand.Reader, priv, ciphertext, nil)
}

// RSADecryptPKCS1v15 decrypt pkcs1 v1.5
func RSADecryptPKCS1v15(PEMPriv []byte, ciphertext []byte) (plaintext []byte, err error) {
	priv, err := cw.ParsePemRsaPrivateKey(PEMPriv)
	if err != nil {
		err = errors.Wrap(err, "ParsePemRsaPrivateKey")
		return nil, err
	}

	return rsa.DecryptPKCS1v15(rand.Reader, priv, ciphertext)
}

func RSADecrypt(transform string, PEMPriv []byte, ciphertext []byte) (plaintext []byte, err error) {
	if transform == "RSA/None/PKCS1Padding" {
		return RSADecryptPKCS1v15(PEMPriv, ciphertext)
	} else if transform == "RSA/SHA256/PKCS1_OAEPPadding" {
		return RSADecryptPKCS1OAEP(sha256.New(), PEMPriv, ciphertext)
	} else if transform == "RSA/SHA384/PKCS1_OAEPPadding" {
		return RSADecryptPKCS1OAEP(sha512.New384(), PEMPriv, ciphertext)
	} else if transform == "RSA/SHA512/PKCS1_OAEPPadding" {
		return RSADecryptPKCS1OAEP(sha512.New(), PEMPriv, ciphertext)
	}

	return nil, errors.New("not support transform")
}
