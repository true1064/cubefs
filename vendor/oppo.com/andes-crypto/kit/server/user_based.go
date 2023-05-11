/*
Copyright OPPO Corp. All Rights Reserved.
*/

package server

import (
	"io"

	"oppo.com/andes-crypto/kit/server/engine"
	"oppo.com/andes-crypto/kit/server/errno"
)

// UserBased Based on the user-based encryption and decryption scheme, the encrypted data can
// be decrypted on all devices of the user's same account.
type UserBased struct{}

// Init AndesCryptoKit初始化，完成不同加密方案的秘钥托管配置管理以及认证鉴权。
//
//	@param kmsParam KMS认证鉴权信息，业务方需要到安全密钥控制台申请相关的权限以及下载相关的密钥信息。
//	@return error 如果初始化失败（如KMS鉴权失败），将返回具体的错误信息，否则返回空。
func (u *UserBased) Init(kmsParam KmsParam, env EnvironmentType) *errno.Errno {
	return nil
}

// NewEngineTransCipher 创建传输加密引擎。
//
//	@param cipherKey 传输加密密钥的密文，它在终端侧创建后由AndesCryptoKit的公钥加密再经过base64编码，通过HTTP请求的头部字段"Transfer-Key"传输到云端。
//	@param cipherIv 传输加密IV的密文，它在终端侧创建后由AndesCryptoKit的公钥加密再经过base64编码，通过HTTP请求的头部字段"Transfer-IV"传输到云端。
//	@return *engine.EngineTransCipher
//	@return error 分配引擎出错则返回错误原因，否则返回空。
func (u *UserBased) NewEngineTransCipher(cipherMaterial *string) (*engine.EngineTransCipher, *errno.Errno) {
	return engine.NewEngineTransCipher(nil, nil, true)
}

func (u *UserBased) NewEngineTransCipherStream(cipherMode engine.CipherMode, reader io.Reader) (*engine.EngineTransCipherStream, *errno.Errno) {
	return engine.NewEngineTransCipherStream(nil, nil, true, engine.ENCRYPT_MODE, reader)
}

// NewEngineFileCipher 创建文件加密引擎。
//
//	@param cipherKey 文件加密密钥的密文。如果加密文件时需要重新分配一个加密密钥，则应当传nil。如果需要使用同一个密钥，则传入保存的加密密钥。
//	@return *engine.EngineFileCipher
//	@return error 分配引擎出错则返回错误原因，否则返回空。
func (u *UserBased) NewEngineFileCipher(cipherKey []byte, blockSize uint64) (*engine.EngineFileCipher, []byte, *errno.Errno) {
	return engine.NewEngineFileCipher(nil, nil, blockSize)
}

func (s *UserBased) NewEngineFileCipherStream(cipherKey []byte, blockSize uint64, cipherMode engine.CipherMode, reader io.Reader) (*engine.EngineFileCipherStream, []byte, *errno.Errno) {
	return engine.NewEngineFileCipherStream(nil, cipherKey, blockSize, cipherMode, reader)
}
