/*
Copyright OPPO Corp. All Rights Reserved.
*/

package andescryptokit

import (
	"andescryptokit/engine"
	"andescryptokit/errno"
	"io"
)

// UserBased
type UserBased struct{}

// Init AndesCryptoKit初始化，完成不同加密方案的秘钥托管配置管理以及认证鉴权。
//
//	@param kmsParam KMS认证鉴权信息，业务方需要到安全密钥控制台申请相关的权限以及下载相关的密钥信息。
//	@return error 如果初始化失败（如KMS鉴权失败），将返回具体的错误信息，否则返回空。
func (u *UserBased) Init(kmsParam KmsParam, env EnvironmentType) *errno.Errno {
	return nil
}

func (u *UserBased) NewEngineTransCipherStream(cipherMode engine.CipherMode, reader io.Reader) (*engine.EngineTransCipherStream, *errno.Errno) {
	return engine.NewEngineTransCipherStream(nil, nil, true, engine.ENCRYPT_MODE, reader)
}

func (s *UserBased) NewEngineFileCipherStream(cipherKey []byte, blockSize uint64, cipherMode engine.CipherMode, reader io.Reader) (*engine.EngineFileCipherStream, []byte, *errno.Errno) {
	return engine.NewEngineFileCipherStream(nil, cipherKey, blockSize, cipherMode, reader)
}

func (s *UserBased) NewEngineAesGCMCipher(cipherMaterial []byte) (*engine.EngineAesGCMCipher, *errno.Errno) {
	return engine.NewEngineAesGCMCipher(nil)
}
