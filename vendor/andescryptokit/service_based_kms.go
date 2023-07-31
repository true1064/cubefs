/*
Copyright OPPO Corp. All Rights Reserved.
*/

package andescryptokit

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"

	"andescryptokit/engine"
	"andescryptokit/errno"
	"andescryptokit/osec/go-kms/kms"
	"andescryptokit/types"
)

// var PrivateKey = []byte(`
// -----BEGIN PRIVATE KEY-----
// MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCobTkW5NJm8dy7
// ifGNECfmGeBO5lpTxK7I8f2WNz/qnTg+8JiFkVqvDBAyj0zvfZgzQIon/v4m9dwr
// 9NLu/ZGYwSOCIiBk/UyQJUqo4SHoxeACSst0q6LFs3jG1jIUcIg1hBvsusLYDI7q
// B451FRLLW50MMHICOzKkId0iACWisJXxmB493zGk/er89Kl/Y+4ZG3+HGRL7Ga0E
// IW3HoS6LVH3piT5hGSQqnxoFloKMmAZaj0K7+JSmz/8915yjQ8yYEwdMjv3vxHu7
// HtSbwv01JQy5AiHGMRQb0Up3bje7ToYNL1RXWYDoMUjQ1ZGnHqAgfLYQ7VKLrZaI
// X1EHFDyFAgMBAAECggEBAJh8OoGVrT/ynUtVOlHiXJ0gtEn6l7DbkvruzA3h+4Yn
// zTJ+lTfoP1fX7ho4TwlMhi1wsyQ+4k7XrPxG7WnePKE1yzYYO4fzJAwlFxEKsq6R
// 55trqAP7GKUxNZmfdzu2HjuFXdrXw2vjAsizCrDJ4Xgne2n7ulx2yAR9fD06MNi2
// 2r+MMFhMqkTHo9fzea+b3xWpuXhVFHVKOmcEavWDiQ2MSGvJ7jS50oTi1egWMfSs
// M4Ij7id8rfEixWuNXUV0sYpVryaltFZriKZjxQc2LVHPLbOSwRXqjs1hCbDn9Ai+
// X9ku2X7Qo9+9ZuB8RxTVMez6RLxnrYS/0pYVLZzaZ8kCgYEA2b5d7JLNkfl0YuLq
// A4OplkwTUF4+AYqiTAlC0y3QQzcWnXRU/w6ZKSn+KmnyXBXAQISSDjjQ7VrCLccR
// KzbLVix9sC/RLELEc5ZMZPrOMd0S3CF7a8+tp7r1OW5le+SMDEOqHzzvrwig7hRQ
// ZgYhsvl4dk2KMa2JxTMPRvTQOcsCgYEAxgSvu08lqqyxpU3ucPy7FQN4Ng3TtD5G
// w4Kt/wbcL7o2/+BKN5RA8H039805Qt4W7bNwxgt9yH5880ESonWeGTZ1DWajNhM0
// NRxJN+XNgO9qPBvIYCKF/eH+u9TmBfxjtU3afG7Son06xt5MV0ddY1PRcSuvgUmi
// up8wAmZh2O8CgYEAlc2CkN/uzh7xE5dJqGFMqzprjz9HKYhXYDh+4QsD3TppNKp0
// T4WbmdZqJoP9WZ0dR0XtthsgoiturE/oX/KysfaqAizjm6/TuDIPHOnwMh4Ge6wN
// XX7j2iGl1H0/FJ2IPGfRAuzJeYJWNXWEWqydQqSfW8S87rCVuDYIrKiBGUECgYB2
// xvLeKRCP+vyoCkH/dimF5kniKpMVZ9GsjqNt+SB0ZH9/Jnt+MShu2L2Mn4Y8bNlW
// Ba+cq+HsNKsggqT53BFUUE0QF8PIuOY1AV2N8QM+1t1jZsrfl3XGHxxccMz1RDpO
// 03896n/gRbvO9CLYq48B+JIGD008AP5icQAsQFtq8wKBgQDIplZxtn8ekTLfqw3O
// VJLoHUYQm6KwkvyPriSt65g6muhoN1V/Gji8KpG6y+fwW5h+IGhk3P9s/z7Et6kR
// mSJDejld3gg9gYOUOiNWwdLa06u8uHMld6/Z4hznL41NKvUt02+aOarkSYb/1BQh
// Glgo41qGEnphA0PLAtt8eqFsKA==
// -----END PRIVATE KEY-----
// `)

// ServiceBasedKMS 服务级KMS加密方案，端云数据传输基于数字信封，云侧数据存储基于KMS，托管文件密钥。
type ServiceBasedKMS struct {

	// configure 配置参数
	configure *types.Configure

	// kmsEx KMS 3.0
	//kmsEx *okey.KmsEx

	// kmsClient KMS NXG
	kmsClient *kms.KMSClient
}

// NewServiceBasedKMS 创建服务级KMS加密方案。
//  @param configure 配置信息。
//  @return *ServiceBasedKMS 服务级KMS加密方案对象。
//  @return *errno.Errno 如果出错，返回错误码以及错误信息。
func NewServiceBasedKMS(configure *types.Configure) (*ServiceBasedKMS, *errno.Errno) {
	s := &ServiceBasedKMS{}
	s.configure = configure

	// 初始化KMS3.0
	err := s.initKMSEx(configure.AuthParam, configure.Environment)
	if err != errno.OK {
		return nil, err
	}

	// 初始化KMS NXG
	err = s.initKmsNxg(configure.AuthParam, configure.Environment)
	if err != errno.OK {
		return nil, err
	}

	return s, errno.OK
}

// NewEngineTransCipher 创建传输加密引擎。
//  @param cipherMode 工作模式：加密、解密。
//  @param cipherMaterial 加密材料，它从终端产生。解密模式下，如果为空，将会从reader读取，如果不为空但解析加密材料失败，则返回错误；加密模式下，如果为空则直接返回错误。
//  @param reader 待处理的数据流。
//  @return *engine.EngineTransCipher 传输加密引擎对象。
//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
func (s *ServiceBasedKMS) NewEngineTransCipher(cipherMode types.CipherMode, cipherMaterial []byte, reader io.Reader) (*engine.EngineTransCipher, *errno.Errno) {
	// 如果加密材料为空，则从reader里读取加密材料。
	if cipherMaterial == nil {
		if reader == nil {
			return nil, errno.TransCipherMaterialNilError
		}

		// 加密模式下，必须显示通过参数传递加密材料。
		if cipherMode == types.ENCRYPT_MODE {
			return nil, errno.TransCipherMaterialNilError.Append("ENCRYPT_MODE without cipher material/")
		}

		// 读取加密材料，长度固定字节
		cipherMaterial = make([]byte, types.SERVICE_BASED_KMS_TRNAS_CIPHER_MATERIAL_LEN)
		_, err := io.ReadFull(reader, cipherMaterial)
		if err != nil {
			return nil, errno.TransCipherMaterialUnexpectedEOfError.Append(err.Error())
		}
	}

	// 加密材料protobuf反序列化
	material := CipherMaterial{}
	err := proto.Unmarshal(cipherMaterial, &material)
	if err != nil {
		return nil, errno.TransCipherMaterialUnmarshalError.Append(err.Error())
	}

	// RSA解密DEK
	//plainDek, err := s.rsaDecrypt(material.GetPublickKeyCipherDEK())
	plainDek, err := s.rsaKmsDecrypt(material.GetPublickKeyCipherDEK())
	if err != nil {
		return nil, errno.TransCipherMaterialRSADecryptError.Append(err.Error())
	}

	// 转换hmac为bool值
	hmac := false
	if material.GetHmac() != 1 {
		hmac = true
	}

	// 如果是加密模式，需要更新IV
	if cipherMode == types.ENCRYPT_MODE {
		if _, err := io.ReadFull(rand.Reader, material.IV); err != nil {
			return nil, errno.TransCipherIVLengthError.Append(err.Error())
		}
	}
	// 序列化加密材料
	cipherMaterial, err = proto.Marshal(&material)
	if err != nil {
		return nil, errno.TransCipherMaterialUnmarshalError.Append(err.Error())
	}

	return engine.NewEngineTransCipher(cipherMode, cipherMaterial, reader, plainDek, material.GetIV(), hmac)
}

// NewEngineFileCipher 创建文件加密引擎。
//  @param cipherMode 工作模式：加密、解密。
//  @param cipherMaterial 加密材料，它能够在终端或者云端产生。服务级KMS加密方案：如果为空，将重新向KMS申请DEK，如果不为空但解析加密材料失败，则返回错误；
//  @param reader 待处理的数据流。
//  @param blockSize 数据分组长度，必须为16的倍数。数据将按设定的分组长度进行分组加密，用户随机解密的最新分段即为该分组大小。
//  @return *engine.EngineFileCipher 文件加密引擎对象。
//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
func (s *ServiceBasedKMS) NewEngineFileCipher(cipherMode types.CipherMode, cipherMaterial []byte, reader io.Reader, blockSize uint64) (*engine.EngineFileCipher, *errno.Errno) {
	// 加密材料为空：解密模式下，加密材料在reader流里。
	if cipherMaterial == nil && cipherMode == types.DECRYPT_MODE {
		if reader == nil {
			return nil, errno.FileCipherDecryptModeMaterialNilError
		}

		// 读取加密材料，长度固定字节
		cipherMaterial = make([]byte, types.SERVICE_BASED_KMS_FILE_CIPHER_MATERIAL_LEN)
		_, err := io.ReadFull(reader, cipherMaterial)
		if err != nil {
			return nil, errno.FileCipherDecryptModeMaterialUnexpectedEOfError.Append(err.Error())
		}
	}

	// 加密材料protobuf反序列化
	material := CipherMaterial{}
	err := proto.Unmarshal(cipherMaterial, &material)
	if err != nil {
		return nil, errno.FileCipherMaterialUnmarshalError.Append(err.Error())
	}

	// 解密模式下，KMS加密材料为空，返回错误。
	kmsCipherDEK := material.GetKMSCipherDEK()
	if kmsCipherDEK == nil && cipherMode == types.DECRYPT_MODE {
		return nil, errno.FileCipherDecryptModeMaterialNilError
	}

	// 去KMS解密或者申请新的DEK
	plainKey, cipherKey, err := s.prepareKey(kmsCipherDEK)
	if plainKey == nil {
		return nil, errno.ServiceBasedDataEncryptKeyError.Append(err.Error())
	}

	// 更新加密材料
	material = CipherMaterial{
		KMSCipherDEK: cipherKey,
	}
	cipherMaterial, _ = proto.Marshal(&material)
	if err != nil {
		return nil, errno.TransCipherMaterialUnmarshalError.Append(err.Error())
	}

	return engine.NewEngineFileCipher(cipherMode, cipherMaterial, reader, blockSize, plainKey)
}

// NewEngineAesGCM NewEngineAesGCMCipher 创建AES-256-GCM加密引擎。
//  @receiver s
//  @param cipherMaterial 加密材料，由端侧生成并传输至云端。
//  @return *engine.EngineAesGCM ES-256-GCM加密引擎对象。
//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
func (s *ServiceBasedKMS) NewEngineAesGCMCipher(cipherMaterial []byte) (*engine.EngineAesGCMCipher, *errno.Errno) {
	// 加密材料protobuf反序列化
	material := CipherMaterial{}
	err := proto.Unmarshal(cipherMaterial, &material)
	if err != nil {
		return nil, errno.TransCipherMaterialUnmarshalError.Append(err.Error())
	}

	// 加密材料RSA解密
	plainDek, err := s.rsaKmsDecrypt(material.GetPublickKeyCipherDEK())
	if err != nil {
		return nil, errno.TransCipherMaterialRSADecryptError.Append(err.Error())
	}

	return engine.NewEngineAesGCMCipher(plainDek)
}

// initKMSEx KMS3.0初始化。
//
//  @receiver s
//  @param authParam AUTH配置参数。
//  @param env 运行环境。
//  @return *errno.Errno 如果出错，返回错误码以及错误信息。
func (s *ServiceBasedKMS) initKMSEx(authParam types.AuthParam, env types.EnvironmentType) *errno.Errno {
	// param := okey.Params{
	// 	AppName: authParam.appName,
	// 	Ak:      authParam.ak,
	// 	Sk:      authParam.sk,
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

// initKmsNxg KMS-NXG初始化。
//
//  @receiver s
//  @param authParam KMS初始化参数。
//  @param env 运行环境。
//  @return *errno.Errno 如果出错，返回错误码以及详细信息。
func (s *ServiceBasedKMS) initKmsNxg(authParam types.AuthParam, env types.EnvironmentType) *errno.Errno {
	var err error
	s.kmsClient, err = kms.New(authParam.AK, authParam.SK, int(env))
	if err != nil {
		return errno.KmsNxgInitError.Append(err.Error())
	}

	return errno.OK
}

// generateDataKey Apply to KMS NXG for a key to encrypt data.
//
//	@receiver service
//	@return *kms.GenerateDataKeyOutput
//	@return error
func (s *ServiceBasedKMS) generateDataKey(keyId *string) (*kms.GenerateDataKeyOutput, error) {
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
func (s *ServiceBasedKMS) prepareKey(key []byte) ([]byte, []byte, error) {
	var err error

	// If key is empty, we need to create a new encryption key.
	if key == nil {
		// Apply for a new encryption key from KMS.
		var generateDataKeyOutput *kms.GenerateDataKeyOutput
		if generateDataKeyOutput, err = s.generateDataKey(&s.configure.CustomMasterKey.FileKeyId); err != nil {
			return nil, nil, err
		}

		// Obtain the plaintext DEK according to the Data Key, which is used to encrypt user data.
		if key, err = base64.StdEncoding.DecodeString(generateDataKeyOutput.Plaintext); err != nil {
			return nil, nil, err
		}

		return key, []byte(generateDataKeyOutput.CiphertextBlob), nil
	}

	// If it is not empty, the ciphertext DEK needs to be decrypted first, and then used to encrypt user data.
	decryptInput := kms.DecryptInput{string(key), "SYMMETRIC_DEFAULT", "", s.configure.CustomMasterKey.FileKeyId}
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
// func (s *ServiceBasedKMS) rsaDecrypt(ciphertext []byte) ([]byte, error) {
// 	block, _ := pem.Decode(PrivateKey)
// 	if block == nil {
// 		return nil, errors.New("private key decode error!")
// 	}

// 	parseResult, err := x509.ParsePKCS8PrivateKey(block.Bytes)
// 	if err != nil {
// 		return nil, err
// 	}

// 	plaintext, err := rsa.DecryptOAEP(sha256.New(), rand.Reader, parseResult.(*rsa.PrivateKey), ciphertext, nil)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return plaintext, nil
// }

func (s *ServiceBasedKMS) rsaKmsDecrypt(ciphertext []byte) ([]byte, error) {
	blob := fmt.Sprintf("{\"version\":1,\"alg\":\"RSAES_OAEP_SHA_256\",\"encrypted_data\":\"%s\"}", base64.StdEncoding.EncodeToString(ciphertext))

	decryptInput := kms.DecryptInput{base64.StdEncoding.EncodeToString([]byte(blob)), "RSAES_OAEP_SHA_256", "", s.configure.CustomMasterKey.TransKeyId}
	decryptOutput, err := s.kmsClient.Decrypt(decryptInput)
	if err != nil {
		return nil, err
	}

	plainDek, err := base64.StdEncoding.DecodeString(decryptOutput.Plaintext)
	if err != nil {
		return nil, err
	}

	return plainDek, nil
}
