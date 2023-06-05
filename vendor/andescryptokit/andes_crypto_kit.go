/*
Copyright OPPO Corp. All Rights Reserved.
*/

// package
package andescryptokit

import (
	"andescryptokit/engine"
	"andescryptokit/errno"
	"io"
)

// AndesCryptoKit defines the crypto proposal, eg service-based, user-based and device based.
type AndesCryptoKit interface {
	// Init AndesCryptoKit初始化，完成不同加密方案的秘钥托管配置管理以及认证鉴权。
	//  @param kmsParam KMS认证鉴权信息，业务方需要到安全密钥控制台申请相关的权限以及下载相关的密钥信息。
	//  @return error 如果初始化失败（如KMS鉴权失败），将返回具体的错误信息，否则返回空。
	Init(kmsParam KmsParam, env EnvironmentType) *errno.Errno

	// NewEngineTransCipher 创建传输加密引擎。
	//  @param cipherMaterial 传输加密引擎使用的加密材料，由客户端生成并通过HTTP请求头部Cipher-Material携带至服务端。
	//  @return *engine.EngineTransCipher 传输加密引擎对象。
	//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
	NewEngineTransCipher(cipherMaterial *string) (*engine.EngineTransCipher, *errno.Errno)

	// NewEngineTransCipherStream 创建流式传输加密引擎。
	//  @param cipherType 加密类型：加密模式和解密模式。
	//  @param reader 数据读取流，如文件流，网络流。
	//  @return *engine.EngineTransCipherStream 流式传输加密引擎对象。
	//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
	NewEngineTransCipherStream(cipherMode engine.CipherMode, reader io.Reader) (*engine.EngineTransCipherStream, *errno.Errno)

	// NewEngineFileCipher 创建文件加密引擎。
	//  @param cipherKey 文件加密密钥的密文。如果加密文件时需要重新分配一个加密密钥，则应当传nil。如果需要使用同一个密钥，则传入第一次创建的密钥的密文。
	//  @param blockSize 待加密或解密数据的分组长度，必须为16的整数倍。当使用EncryptFile接口时，blockSize应当小于或等于每次从IO缓冲区读取的长度。
	//  @return *engine.EngineFileCipher 文件加密引擎对象。
	//  @return []byte 由KMS生成的加密密钥的密文，用户应当存储并妥善保存，丢失将无法解密文件。
	//  @return error 如果失败，返回错误原因以及错误码。
	NewEngineFileCipher(cipherKey []byte, blockSize uint64) (*engine.EngineFileCipher, []byte, *errno.Errno)

	// NewEngineFileCipherStream 创建流式文件加密引擎。
	//  @param cipherKey 文件加密密钥的密文。如果加密文件时需要重新分配一个加密密钥，则应当传nil。如果需要使用同一个密钥，则传入第一次创建的密钥的密文。
	//  @param blockSize 待加密或解密数据的分组长度，必须为16的整数倍。
	//  @param reader 数据读取流，如文件流，网络流。
	//  @return *engine.EngineFileCipher 流式文件加密引擎对象。
	//  @return []byte 由KMS生成的加密密钥的密文，用户应当存储并妥善保存，丢失将无法解密文件。
	//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
	NewEngineFileCipherStream(cipherKey []byte, blockSize uint64, cipherMode engine.CipherMode, reader io.Reader) (*engine.EngineFileCipherStream, []byte, *errno.Errno)
}

// New 获取加密方案对象，加密方案有服务级加密、用户级加密和设备级加密，不同的加密方案的加密秘钥保管方式不一样。
//
//	@param cipherScheme 加密方案。
//	@return 加密方案对象，它是AndesCryptoKit的实现。
func New(cipherScheme CipherScheme) (AndesCryptoKit, *errno.Errno) {
	switch cipherScheme {
	case CipherScheme_Common:
		return nil, errno.UnsupportedCipherSchemeError
	case CipherScheme_Service:
		return &ServiceBased{}, errno.OK
	case CipherScheme_User:
		return &UserBased{}, errno.UnsupportedCipherSchemeError
	case CipherScheme_Device:
		return &DeviceBased{}, errno.UnsupportedCipherSchemeError
	default:
		return nil, errno.InternalError
	}
}
