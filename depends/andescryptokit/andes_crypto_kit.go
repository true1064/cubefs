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

// AndesCryptoKit 是一个被多种加密方案实现的接口， 不同的加密方案的数据读写权限不同，提供创建多种基础加密引擎以完成对数据的加解密。
// 服务级加密：托管秘钥，密钥在云端服务基于KMS生成，用户数据在端侧和云侧均能够解密；
// 用户级加密：托管秘钥，密钥在终端设备基于用户锁屏密码生成，用户数据仅能在用户同一账号的多个端侧解密，无法在云端解密；
// 设备级加密：托管秘钥，密钥在终端设备基于TEE生成，用户数据仅能在用户的单台端侧上解密；。
// 不同的加密方案的数据读写权限不同，都提供创建多种加密引擎的能力。
type AndesCryptoKit interface {

	// Init AndesCryptoKit初始化，完成不同加密方案的秘钥托管配置管理以及认证鉴权。
	//  @param kmsParam KMS认证鉴权信息，业务方需要到安全密钥控制台申请相关的权限以及下载相关的密钥信息。
	//  @return error 如果初始化失败（如KMS鉴权失败），将返回具体的错误信息，否则返回空。
	Init(kmsParam KmsParam, env EnvironmentType) *errno.Errno

	// NewEngineTransCipherStream 创建流式传输加密引擎。
	//  @param cipherType 加密类型：加密模式和解密模式。
	//  @param reader 数据读取流，如文件流，网络流。
	//  @return *engine.EngineTransCipherStream 流式传输加密引擎对象。
	//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
	NewEngineTransCipherStream(cipherMode engine.CipherMode, reader io.Reader) (*engine.EngineTransCipherStream, *errno.Errno)

	// NewEngineFileCipherStream 创建流式文件加密引擎。
	//  @param cipherKey 文件加密密钥的密文。如果加密文件时需要重新分配一个加密密钥，则应当传nil。如果需要使用同一个密钥，则传入第一次创建的密钥的密文。
	//  @param blockSize 待加密或解密数据的分组长度，必须为16的整数倍。
	//  @param reader 数据读取流，如文件流，网络流。
	//  @return *engine.EngineFileCipher 流式文件加密引擎对象。
	//  @return []byte 由KMS生成的加密密钥的密文，用户应当存储并妥善保存，丢失将无法解密文件。
	//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
	NewEngineFileCipherStream(cipherKey []byte, blockSize uint64, cipherMode engine.CipherMode, reader io.Reader) (*engine.EngineFileCipherStream, []byte, *errno.Errno)

	// NewEngineAesGCMCipher 创建AES-256-GCM加密引擎。
	//  @param cipherMaterial 加密材料，由端侧生成并传输至云端。
	//  @return *engine.EngineAesGCM ES-256-GCM加密引擎对象。
	//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
	NewEngineAesGCMCipher(cipherMaterial []byte) (*engine.EngineAesGCMCipher, *errno.Errno)
}

// EngineStream 是一个被多种密码基础引擎实现的接口，引擎结合不同的加密算法以适应不同的使用场景。如：
// 文件加密引擎：加密算法使用AES-256-XTS，具有高安全、高性能的特性，支持数据的分段随机加解密；
// 传输加密引擎：加密算法使用AES-256-CTR，具有高安全、高性能的特性，支持流式数据的加解密；
type EngineStream interface {

	// Read 读取一部分明（密）文数据。
	//  @param p 存储读取的明（密）文数据。
	//  @return int 成功读取的明（密）文数据的长度。
	//  @return error 返回错误信息或文件结束标示EOF。
	Read(p []byte) (int, error)
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
