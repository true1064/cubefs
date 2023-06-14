/*
Copyright OPPO Corp. All Rights Reserved.
*/

package andescryptokit

import (
	"andescryptokit/engine"
	"andescryptokit/errno"
	"io"
)

// DeviceBased Based on the device-based encryption scheme, the encrypted data can only be decrypted on the same device.
type DeviceBased struct{}

// Init AndesCryptoKit初始化，完成不同加密方案的秘钥托管配置管理以及认证鉴权。
//
//	@param kmsParam KMS认证鉴权信息，业务方需要到安全密钥控制台申请相关的权限以及下载相关的密钥信息。
//	@return error 如果初始化失败（如KMS鉴权失败），将返回具体的错误信息，否则返回空。
func (d *DeviceBased) Init(kmsParam KmsParam, env EnvironmentType) *errno.Errno {
	return nil
}

func (d *DeviceBased) NewEngineTransCipherStream(cipherMode engine.CipherMode, reader io.Reader) (*engine.EngineTransCipherStream, *errno.Errno) {
	return engine.NewEngineTransCipherStream(nil, nil, true, engine.ENCRYPT_MODE, reader)
}

func (s *DeviceBased) NewEngineFileCipherStream(cipherKey []byte, blockSize uint64, cipherMode engine.CipherMode, reader io.Reader) (*engine.EngineFileCipherStream, []byte, *errno.Errno) {
	return engine.NewEngineFileCipherStream(nil, cipherKey, blockSize, cipherMode, reader)
}

func (s *DeviceBased) NewEngineAesGCMCipher(cipherMaterial []byte) (*engine.EngineAesGCMCipher, *errno.Errno) {
	return engine.NewEngineAesGCMCipher(nil)
}
