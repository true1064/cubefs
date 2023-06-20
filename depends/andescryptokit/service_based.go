/*
Copyright OPPO Corp. All Rights Reserved.
*/

package andescryptokit

import (
	"andescryptokit/engine"
	"andescryptokit/errno"
	"andescryptokit/osec/go-kms/kms"
	"andescryptokit/osec/seckit-go/okey"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"io"

	"github.com/golang/protobuf/proto"
)

const (
	AuthUrl = "https://datasec-auth-cn.heytapmobi.com"
)

// KmsExUrl
var KmsExUrl = map[EnvironmentType]string{
	EnvironmentDevelop: "http://kmsex-dev.wanyol.com",
	EnvironmentTest:    "http://kmsex-test.wanyol.com",
	EnvironmentProduct: "http://kmsex-prod.wanyol.com",
}

// KmsNxgUrl
var KmsNxgUrl = map[EnvironmentType]string{
	EnvironmentDevelop: "http://kms-dev.wanyol.com",
	EnvironmentTest:    "http://kms-test.wanyol.com",
	EnvironmentProduct: "http://datasec-kms-cn.oppo.local",
}

var PrivateKey = []byte(`
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC29EzLc1nG7PdN
eIUHaj/uhTS7JRmavnIQKPlQcQgfcKm5kotX2dNcVmBYlHDz/oalW6RaBNKr8UBG
VuptgXz92DNo8xZDGzZUHH3Pa1Gbln+0AjHKzPW+vy8s0K1DG+UkR9epEGWmLrBQ
gvUwTsB1THjyebzgXIVyyoiLvmcieT1M7D2osatOK7rd1RMnJ6u8yp0Say6Xm4f3
bUoKzDugxEVDcPIijPW+1yZVYGrZxiPM/dtH96ehBX9D0qeNXk42k1VPPJonfTZB
wDjJBrL1Y0eBwHgefpGjY/a5aau8j8WivYFdjGjnWgc/h6oNgMRUJ3DaoAZASXCg
S9d9xFZ/AgMBAAECggEBAIWfpL8rvrR1uqIQlki2J3+UNtFA5ZSJ6FE5O+6uv5oG
9U+eYruFUsQsKi5ILL9odPstFbrRrvT6PVGihZH5c0sLr3DFqsrUgzenn5Pw9CfJ
IfhdafSPAiWRCWmX+BP9ubxn5HGMklFv5ELJgz8HJbYGZofitjrAGI+gsV4vcLBR
04uz7tgS876+hybPmUEtobwdiTl8TH+BJHGppEjh+USzzSvDekqkKk4OGXC1TaaC
trocDnR5pwoqdq56xlpNe96IANDqO20CUugVQhrVszRWitDBUO8nhJipFXCwzQCU
qpGiUkYiGqixj42aAQutxPNLSTR/OR1SwRcBdqAF57ECgYEA6cEKKionzUK33YDk
nvhcxIgYhrop8Y19BKuLKj8IFqtJlVjTPqI5H/rbVNNctchgF20kJVQzVEmC5tHx
UQ592EJythHqifKjtNonkCAJ5WUZcTPRUXK08k6hiaDmVaqhF3CUNOMt+zgvNCwj
fMjOh1xt9WSAvk25Knt0aN++QrsCgYEAyF2fmCRTOv/Ku1SJ2jZ6iQY2Yq4uvoWl
BaEaIapAqW6JbXuPqsL240joQdPp7/xFYp25xJUMbvYT/+LLscCsCUFjniz4fR9f
UcrRAdNr80z/d/x3/k9+2uCxc0IW0lMAR+HN4v9CsCitvO1+8l/9StXbqmua71sf
cd9i0g4RKQ0CgYBzM9M5opsJG0eYoAyMmGTH6zcDNz6ysIObyJuGj5gB7BsQwr0h
kjvLGgv8i0qWwJEzGnQE/bFqaexcq71dsGGvMlhsiPnpUK8D07xJ2LBn4OKgGuSf
WoP5I22TcbvzHmvULISuaARiWndRJCu9NQ4sQg5jMUwN/ioy2LMveI3BPwKBgHm5
30Zn8ySp1/l8/47RgBoU6x+CYKWgvcOwvxZqv4Pvwo///CUq8yDb3zZ2zu3cXi7u
UbirMHWNDFNt7oAb5Khu0F19Rq2FTLx9MgMg9blHMwErIZ8fnprM3SF/qiu2/zms
Zg+dMl+hJwPbaT9Ir7IyQJTFcMgvnOfqZbflkelVAoGBAOlhXN3Mka2Lhvq3i3NY
dn3RAqxn4ERiB1Pz2ldPCfSVVreUE6OlQSwkBoztN9gEyomkqDvcMyi9NHliogIh
fpUTMymWVvnd9MGTjh/f2C5gGmFCXMV/OVu6/AlyYZx7l/w/1uWLLPkjUiKAFi7b
C8CRvNwziVocvBF/bQ/1dG/c
-----END PRIVATE KEY-----
`)

// var AuthUrl = map[int]string{
// 	EnvCodeDEV:  "http://kms-dev.wanyol.com",
// 	EnvCodeTEST: "http://kms-test.wanyol.com",
// 	EnvCodePROD: "http://datasec-kms-cn.oppo.local",
// }

// ServiceBased 服务级加密方案，端云数据传输基于数字信封，云侧数据存储基于KMS，托管文件密钥。
type ServiceBased struct {

	// keyId KMS NXG KeyID
	keyId string

	// kmsEx KMS 3.0
	kmsEx *okey.KmsEx

	// kmsClient KMS NXG
	kmsClient *kms.KMSClient
}

// initKMS To initialize the KMS3.0 service, you need to configure AK/SK on the cloud platform in advance.
//
//	@receiver service
//	@return error
func (s *ServiceBased) initKMSEx(kmsParam *KmsParam, env EnvironmentType) *errno.Errno {
	// param := okey.Params{
	// 	AppName: kmsParam.AppName,
	// 	Ak:      kmsParam.AK,
	// 	Sk:      kmsParam.SK,
	// 	KmsUrl:  KmsExUrl[env],
	// 	AuthUrl: AuthUrl,
	// }

	// var err error
	// if s.kmsEx, err = okey.GetInstance(param); err != nil {
	// 	return errno.KmsParamError.Append(err.Error())
	// }

	// if err = s.kmsEx.Init(); err != nil {
	// 	return errno.KmsInitError.Append(err.Error())
	// }

	return errno.OK
}

// initKmsNxg To initialize the KMS NXG service, you need to configure AK/SK on the cloud platform in advance.
//
//	@receiver service
//	@return error
func (s *ServiceBased) initKmsNxg(kmsParam *KmsParam, env EnvironmentType) *errno.Errno {
	var err error
	if s.kmsClient, err = kms.New(kmsParam.AK, kmsParam.SK, int(env)); err != nil {
		return errno.KmsNxgInitError.Append(err.Error())
	}

	return errno.OK
}

// generateDataKey Apply to KMS NXG for a key to encrypt data.
//
//	@receiver service
//	@return *kms.GenerateDataKeyOutput
//	@return error
func (s *ServiceBased) generateDataKey(keyId *string) (*kms.GenerateDataKeyOutput, error) {
	var err error
	var generateDataKeyOutput *kms.GenerateDataKeyOutput

	args := kms.GenerateDataKeyInput{*keyId, "", "AES_256", 0}
	if generateDataKeyOutput, err = s.kmsClient.GenerateDataKey(args); err != nil {
		return nil, err
	}

	return generateDataKeyOutput, nil
}

// prepareKey Determine whether to create a new key or use the current key according to the key passed in by the user.
//
//	@receiver service
//	@param key The ciphertext of the encryption key, which is encoded by base64, if it is empty, you need to apply for a new encryption key from KMS.
//	@return []byte The plaintext of the encryption key.
//	@return []byte The ciphertext of the encryption key, which is base64 encoded.
func (s *ServiceBased) prepareKey(key []byte) ([]byte, []byte, error) {
	var err error

	// If key is empty, we need to create a new encryption key.
	if key == nil {
		// Apply for a new encryption key from KMS.
		var generateDataKeyOutput *kms.GenerateDataKeyOutput
		if generateDataKeyOutput, err = s.generateDataKey(&s.keyId); err != nil {
			return nil, nil, err
		}

		// Obtain the plaintext DEK according to the Data Key, which is used to encrypt user data.
		if key, err = base64.StdEncoding.DecodeString(generateDataKeyOutput.Plaintext); err != nil {
			return nil, nil, err
		}

		return key, []byte(generateDataKeyOutput.CiphertextBlob), nil
	}

	// If it is not empty, the ciphertext DEK needs to be decrypted first, and then used to encrypt user data.
	decryptInput := kms.DecryptInput{string(key), "SYMMETRIC_DEFAULT", "", s.keyId}
	var decryptOutput *kms.DecryptOutput
	if decryptOutput, err = s.kmsClient.Decrypt(decryptInput); err != nil {
		return nil, nil, err
	}

	var plainKey []byte
	if plainKey, err = base64.StdEncoding.DecodeString(decryptOutput.Plaintext); err != nil {
		return nil, nil, err
	}
	return plainKey, key, nil
}

// rsaDecrypt RSA解密加密材料。
//
//	@receiver s
//	@param ciphertext 加密材料密文。
//	@return []byte 加密材料明文。
//	@return error 如果出错，返回错误码以及详细信息。
func (s *ServiceBased) rsaDecrypt(ciphertext []byte) ([]byte, error) {
	block, _ := pem.Decode(PrivateKey)
	if block == nil {
		return nil, errors.New("private key decode error!")
	}

	parseResult, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	plaintext, err := rsa.DecryptOAEP(sha256.New(), rand.Reader, parseResult.(*rsa.PrivateKey), ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}

// Init AndesCryptoKit初始化，完成不同加密方案的秘钥托管配置管理以及认证鉴权。
//
//	@param kmsParam KMS认证鉴权信息，业务方需要到安全密钥控制台申请相关的权限以及下载相关的密钥信息。
//	@return error 如果初始化失败（如KMS鉴权失败），将返回具体的错误信息，否则返回空。
func (s *ServiceBased) Init(kmsParam KmsParam, env EnvironmentType) *errno.Errno {
	if err := s.initKMSEx(&kmsParam, env); err.Code() != 0 {
		return err
	} else if err = s.initKmsNxg(&kmsParam, env); err.Code() != 0 {
		return err
	}

	s.keyId = kmsParam.KeyId

	return errno.OK
}

// NewEngineTransCipherStream 创建流式传输加密引擎。
//  @receiver s
//  @param cipherType 加密类型：加密模式和解密模式。
//  @param reader 数据读取流。
//  @return *engine.EngineTransCipherStream 流式传输加密引擎对象。
//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
func (s *ServiceBased) NewEngineTransCipherStream(cipherMode engine.CipherMode, reader io.Reader) (*engine.EngineTransCipherStream, *errno.Errno) {
	if reader == nil {
		return nil, errno.TransCipherMaterialNilError
	}

	// 读取加密材料，长度固定为344字节：RSA-256加密后密文长度为256byte，base64编码后长度固定为344字节。
	cipherMaterialBytes := make([]byte, 375)
	_, err := io.ReadFull(reader, cipherMaterialBytes)
	if err != nil {
		return nil, errno.TransCipherMaterialUnexpectedEOfError.Append(err.Error())
	}

	// 加密材料protobuf反序列化
	material := CipherMaterial{}
	err = proto.Unmarshal(cipherMaterialBytes, &material)
	if err != nil {
		return nil, errno.TransCipherMaterialUnmarshalError.Append(err.Error())
	}

	// 加密材料RSA解密
	DEK, err := base64.StdEncoding.DecodeString(material.GetPublickKeyCipherDEK())
	if err != nil {
		return nil, errno.TransCipherMaterialBase64DecodeError.Append(err.Error())
	}
	plainDek, err := s.rsaDecrypt(DEK)
	if err != nil {
		return nil, errno.TransCipherMaterialRSADecryptError.Append(err.Error())
	}

	IV, err := base64.StdEncoding.DecodeString(material.GetIV())
	if err != nil {
		return nil, errno.TransCipherMaterialBase64DecodeError.Append(err.Error())
	}

	return engine.NewEngineTransCipherStream(plainDek, IV, material.GetHmac(), cipherMode, reader)
}

// NewEngineFileCipherStream 创建流式文件加密引擎。
//  @param cipherKey 文件加密密钥的密文。如果加密文件时需要重新分配一个加密密钥，则应当传nil。如果需要使用同一个密钥，则传入第一次创建的密钥的密文。
//  @param blockSize 待加密或解密数据的分组长度，必须为16的整数倍。
//  @param reader 数据读取流，如文件流，网络流。
//  @return *engine.EngineFileCipher 流式文件加密引擎对象。
//  @return []byte 由KMS生成的加密密钥的密文，用户应当存储并妥善保存，丢失将无法解密文件。
//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
func (s *ServiceBased) NewEngineFileCipherStream(cipherKey []byte, blockSize uint64, cipherMode engine.CipherMode, reader io.Reader) (*engine.EngineFileCipherStream, []byte, *errno.Errno) {
	plainKey, cipherKey, err := s.prepareKey(cipherKey)
	if plainKey == nil {
		return nil, nil, errno.ServiceBasedDataEncryptKeyError.Append(err.Error())
	}

	return engine.NewEngineFileCipherStream(plainKey, cipherKey, blockSize, cipherMode, reader)
}

// NewEngineAesGCM NewEngineAesGCMCipher 创建AES-256-GCM加密引擎。
//  @receiver s
//  @param cipherMaterial 加密材料，由端侧生成并传输至云端。
//  @return *engine.EngineAesGCM ES-256-GCM加密引擎对象。
//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
func (s *ServiceBased) NewEngineAesGCMCipher(cipherMaterial []byte) (*engine.EngineAesGCMCipher, *errno.Errno) {
	// 加密材料protobuf反序列化
	material := CipherMaterial{}
	err := proto.Unmarshal(cipherMaterial, &material)
	if err != nil {
		return nil, errno.TransCipherMaterialUnmarshalError.Append(err.Error())
	}

	DEK, err := base64.StdEncoding.DecodeString(material.GetPublickKeyCipherDEK())
	if err != nil {
		return nil, errno.TransCipherMaterialBase64DecodeError.Append(err.Error())
	}

	// 加密材料RSA解密
	plainDek, err := s.rsaDecrypt(DEK)
	if err != nil {
		return nil, errno.TransCipherMaterialRSADecryptError.Append(err.Error())
	}

	return engine.NewEngineAesGCMCipher(plainDek)
}
