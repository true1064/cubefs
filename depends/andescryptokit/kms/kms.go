package kms

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"fmt"

	kmsNxg "andescryptokit/osec/go-kms/kms"

	"andescryptokit/errno"
	"andescryptokit/types"
)

// KMSClient KMS NXG客户端
type KMSNxgClient struct {
	// kmsClient KMS NXG
	kMSNxgClient *kmsNxg.KMSClient
}

// NewKMSClient 创建KMS NXG客户端
//  @param authParam 鉴权参数
//  @param env 使用环境
//  @return *KMSClient KMS NXG 客户端指针
//  @return *errno.Errno 错误码以及错误信息
func NewKMSClient(authParam *types.AuthParam, env types.EnvironmentType) (*KMSNxgClient, *errno.Errno) {
	kmsClient, err := kmsNxg.New(authParam.AK, authParam.SK, int(env))
	if err != nil {
		return nil, errno.KmsNxgInitError.Append(err.Error())
	}

	return &KMSNxgClient{
		kMSNxgClient: kmsClient,
	}, errno.OK
}

// Decrypt 非对称解密
//  @receiver s
//  @param ciphertext 待解密的密文
//  @param keyId 用户KMS主密钥ID
//  @return []byte 明文
//  @return error 错误码以及错误信息
func (k *KMSNxgClient) Decrypt(ciphertext []byte, keyId string) ([]byte, error) {
	blob := fmt.Sprintf("{\"version\":1,\"alg\":\"RSAES_OAEP_SHA_256\",\"encrypted_data\":\"%s\"}", base64.StdEncoding.EncodeToString(ciphertext))

	decryptInput := kmsNxg.DecryptInput{
		CiphertextBlob:      base64.StdEncoding.EncodeToString([]byte(blob)),
		EncryptionAlgorithm: "RSAES_OAEP_SHA_256",
		EncryptionContext:   "",
		KeyId:               keyId}

	decryptOutput, err := k.kMSNxgClient.Decrypt(decryptInput)
	if err != nil {
		return nil, err
	}

	plainDek, err := base64.StdEncoding.DecodeString(decryptOutput.Plaintext)
	if err != nil {
		return nil, err
	}

	return plainDek, nil
}

// DecryptBlob 从KMS解密数据。
//  @receiver k
//  @param ciphretextBlob 待解密的数据
//  @param keyId 密钥ID
//  @return *string 解密成功的数据
//  @return *errno.Errno 错误信息以及错误码
func (k *KMSNxgClient) DecryptBlob(ciphretextBlob string, keyId string) (*string, *errno.Errno) {
	decryptInput := kmsNxg.DecryptInput{
		CiphertextBlob:      ciphretextBlob,
		EncryptionAlgorithm: "SYMMETRIC_DEFAULT",
		EncryptionContext:   "",
		KeyId:               keyId}

	decryptOutput, err := k.kMSNxgClient.Decrypt(decryptInput)
	if err != nil {
		return nil, errno.TransCipherPrivateKeyDecryptError.Append(err.Error())
	}

	return &decryptOutput.Plaintext, errno.OK
}

// GenerateDEK 从KMS生成数据密钥或在解密数据密钥
//  @receiver k
//  @param cipherDek 主密钥密文，如果为空，则从KMS重新生成，如果不为空，则解密。
//  @param keyId 用户KMS主密钥ID
//  @return []byte 数据密钥明文
//  @return []byte 数据密钥密文
//  @return *errno.Errno 错误信息以及错误码
func (k *KMSNxgClient) GenerateDEK(cipherDek []byte, keyId string) ([]byte, []byte, *errno.Errno) {
	// DEK为空，就从KMS重新分配
	if cipherDek == nil {
		// KMS生成DEK
		generateDataKeyOutput, err := k.kMSNxgClient.GenerateDataKey(kmsNxg.GenerateDataKeyInput{
			KeyId:             keyId,
			EncryptionContext: "",
			KeySpec:           "AES_256",
			NumberOfBytes:     0})
		if err != nil {
			return nil, nil, errno.KmsNxgGernerateDataKeyError.Append(err.Error())
		}

		// DEK转成byte
		plainDek, err := base64.StdEncoding.DecodeString(generateDataKeyOutput.Plaintext)
		if err != nil {
			return nil, nil, errno.KmsNxgDataKeyBase64DecodeError.Append(err.Error())
		}

		return plainDek, []byte(generateDataKeyOutput.CiphertextBlob), errno.OK
	}

	// 如果DEK不为空，就使用KMS解密DEK
	decryptOutput, err := k.kMSNxgClient.Decrypt(kmsNxg.DecryptInput{
		CiphertextBlob:      string(cipherDek),
		EncryptionAlgorithm: "SYMMETRIC_DEFAULT",
		KeyId:               keyId,
	})
	if err != nil {
		return nil, nil, errno.KmsNxgDecryptDataKeyError.Append(err.Error())
	}

	// DEK转成byte
	plainDek, err := base64.StdEncoding.DecodeString(decryptOutput.Plaintext)
	if err != nil {
		return nil, nil, errno.KmsNxgDataKeyBase64DecodeError.Append(err.Error())
	}

	return plainDek, cipherDek, errno.OK
}

// RsaDecrypt RSA解密数据
//  @receiver k
//  @param ciphertext 被私钥加密的数据
//  @param privateKey 私钥明文
//  @return []byte 数据明文
//  @return *errno.Errno 错误信息以及错误码
func (k *KMSNxgClient) RsaDecrypt(ciphertext []byte, privateKey string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(privateKey)
	if err != nil {
		return nil, err
	}

	parseResult, err := x509.ParsePKCS8PrivateKey([]byte(key))
	if err != nil {
		return nil, err
	}

	plaintext, err := rsa.DecryptOAEP(sha256.New(), rand.Reader, parseResult.(*rsa.PrivateKey), ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}
