/*
Copyright OPPO Corp. All Rights Reserved.
*/

// package
package andescryptokit

import (
	"io"

	"andescryptokit/engine"
	"andescryptokit/errno"
	"andescryptokit/types"
)

// AndesCryptoKit 是一个被多种加密方案实现的接口， 不同的加密方案的数据读写权限不同，提供创建多种基础加密引擎以完成对数据的加解密。
// 服务级加密：托管秘钥，密钥在云端服务基于KMS生成，用户数据在端侧和云侧均能够解密；
// 用户级加密：托管秘钥，密钥在终端设备基于用户锁屏密码生成，用户数据仅能在用户同一账号的多个端侧解密，无法在云端解密；
// 设备级加密：托管秘钥，密钥在终端设备基于TEE生成，用户数据仅能在用户的单台端侧上解密；。
// 不同的加密方案的数据读写权限不同，都提供创建多种加密引擎的能力。
type AndesCryptoKit interface {

	// NewEngineTransCipher 创建传输加密引擎。
	//  @param cipherMode 工作模式：加密、解密。
	//  @param cipherMaterial 加密材料，它从终端产生。解密模式下，如果为空，将会从reader读取，如果不为空但解析加密材料失败，则返回错误；加密模式下，如果为空则直接返回错误。
	//  @param reader 待处理的数据流。
	//  @return *engine.EngineTransCipher 传输加密引擎对象。
	//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
	NewEngineTransCipher(cipherMode types.CipherMode, cipherMaterial []byte, reader io.Reader) (*engine.EngineTransCipher, *errno.Errno)

	// NewEngineFileCipher 创建文件加密引擎。
	//  @param cipherMode 工作模式：加密、解密。
	//  @param cipherMaterial 加密材料，它能够在终端或者云端产生。服务级KMS加密方案：如果为空，将重新向KMS申请DEK，如果不为空但解析加密材料失败，则返回错误；
	//  服务级TEE加密方案：如果为空，将会从reader读取，如果不为空但解析加密材料失败，则返回错误。
	//  @param reader 待处理的数据流。
	//  @param blockSize 数据分组长度，必须为16的倍数。数据将按设定的分组长度进行分组加密，用户随机解密的最新分段即为该分组大小。
	//  @return *engine.EngineFileCipher 文件加密引擎对象。
	//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
	NewEngineFileCipher(cipherMode types.CipherMode, cipherMaterial []byte, reader io.Reader, blockSize uint64) (*engine.EngineFileCipher, *errno.Errno)

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
func New(cipherScheme types.CipherScheme, configure types.Configure) (AndesCryptoKit, *errno.Errno) {
	switch cipherScheme {
	case types.CipherScheme_Common:
		return nil, errno.UnsupportedCipherSchemeError
	case types.CipherScheme_ServiceBasedKMS:
		return NewServiceBasedKMS(&configure)
	case types.CipherScheme_ServiceBasedTEE:
		return NewServiceBasedKMS(&configure)
	case types.CipherScheme_User:
		return &UserBased{}, errno.UnsupportedCipherSchemeError
	case types.CipherScheme_Device:
		return &DeviceBased{}, errno.UnsupportedCipherSchemeError
	default:
		return nil, errno.InternalError
	}
}
