/*
Copyright OPPO Corp. All Rights Reserved.
*/

package andescryptokit

import (
	"crypto/rand"
	"io"

	"github.com/golang/protobuf/proto"

	"andescryptokit/engine"
	"andescryptokit/errno"
	"andescryptokit/kms"
	"andescryptokit/types"
)

// ServiceBasedKMS 服务级KMS加密方案，端云数据传输基于数字信封，云侧数据存储基于KMS，托管文件密钥。
type ServiceBasedKMS struct {

	// configure 配置参数
	configure *types.Configure

	// kmsClient KMS NXG
	kmsNxg *kms.KMSNxgClient

	// plainPrivateKey 私钥明文，应用启动时就会将传入的密文解密成明文。
	plainPrivateKey string
}

// NewServiceBasedKMS 创建服务级KMS加密方案。
//  @param configure 配置信息。
//  @return *ServiceBasedKMS 服务级KMS加密方案对象。
//  @return *errno.Errno 如果出错，返回错误码以及错误信息。
func NewServiceBasedKMS(configure *types.Configure) (*ServiceBasedKMS, *errno.Errno) {
	// 连接KMS
	kmsNxg, err := kms.NewKMSClient(&configure.AuthParam, configure.Environment)
	if err != errno.OK {
		return nil, err
	}

	// 【CFA性能优化】解密私钥
	plainPrivateKey, err := kmsNxg.DecryptBlob(configure.CustomMasterKey.CipherPrivateKey, configure.CustomMasterKey.FileKeyId)
	if err != errno.OK {
		return nil, err
	}

	return &ServiceBasedKMS{
		configure:       configure,
		kmsNxg:          kmsNxg,
		plainPrivateKey: *plainPrivateKey,
	}, errno.OK
}

// NewEngineTransCipher 创建传输加密引擎。
//  @param cipherMode 工作模式：加密、解密。
//  @param cipherMaterial 加密材料，它从终端产生。解密模式下，如果为空，将会从reader读取，如果不为空但解析加密材料失败，则返回错误；加密模式下，如果为空则直接返回错误。
//  @param reader 待处理的数据流。
//  @return *engine.EngineTransCipher 传输加密引擎对象。
//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
func (s *ServiceBasedKMS) NewEngineTransCipher(cipherMode types.CipherMode, cipherMaterial []byte, reader io.Reader) (*engine.EngineTransCipher, *errno.Errno) {
	// 云侧不产生加密材料，因此reader不能为空
	if reader == nil {
		return nil, errno.TransCipherStreamModeNilStreamError
	}

	// 如果加密材料为空，则从reader里读取加密材料。
	if cipherMaterial == nil {
		// 加密模式下，必须显示通过参数传递加密材料。
		if cipherMode == types.ENCRYPT_MODE {
			return nil, errno.TransCipherMaterialNilError.Append("ENCRYPT_MODE without cipher material.")
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

	// KMS RSA解密DEK
	plainDek, err := s.kmsNxg.RsaDecrypt(material.GetPublickKeyCipherDEK(), s.plainPrivateKey)
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
		iv := make([]byte, types.AES_256_CTR_IV_LEN)
		if _, err := io.ReadFull(rand.Reader, iv); err != nil {
			return nil, errno.TransCipherIVLengthError.Append(err.Error())
		}
		material.IV = iv
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
	// 云侧不产生加密材料，因此reader不能为空
	if reader == nil {
		return nil, errno.FileCipherStreamModeNilStreamError
	}

	// 如果加密材料为空，并且为解密模式，则从reader里读取加密材料。
	if cipherMaterial == nil && cipherMode == types.DECRYPT_MODE {
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
	plainDek, cipherDek, err := s.kmsNxg.GenerateDEK(kmsCipherDEK, s.configure.CustomMasterKey.FileKeyId)
	if plainDek == nil {
		return nil, errno.FileCipherGernerateDekError.Append(err.Error())
	}

	// 更新加密材料
	cipherMaterial, err = proto.Marshal(&CipherMaterial{
		KMSCipherDEK: cipherDek,
	})
	if err != nil {
		return nil, errno.FileCipherMaterialMarshalError.Append(err.Error())
	}

	return engine.NewEngineFileCipher(cipherMode, cipherMaterial, reader, blockSize, plainDek)
}

// NewEngineAesGCM NewEngineAesGCMCipher 创建AES-256-GCM加密引擎。
//  @receiver s
//  @param cipherMaterial 加密材料，由端侧生成并传输至云端。
//  @return *engine.EngineAesGCM ES-256-GCM加密引擎对象。
//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
func (s *ServiceBasedKMS) NewEngineAesGCMCipher(cipherMaterial []byte) (*engine.EngineAesGCMCipher, *errno.Errno) {
	if cipherMaterial == nil {
		return nil, errno.TransCipherMaterialNilError
	}

	// 加密材料protobuf反序列化
	material := CipherMaterial{}
	err := proto.Unmarshal(cipherMaterial, &material)
	if err != nil {
		return nil, errno.TransCipherMaterialUnmarshalError.Append(err.Error())
	}

	// 加密材料RSA解密
	plainDek, err := s.kmsNxg.RsaDecrypt(material.GetPublickKeyCipherDEK(), s.plainPrivateKey)
	if err != nil {
		return nil, errno.TransCipherMaterialRSADecryptError.Append(err.Error())
	}

	return engine.NewEngineAesGCMCipher(plainDek)
}
