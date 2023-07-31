/*
Copyright OPPO Corp. All Rights Reserved.
*/

package andescryptokit

import (
	"io"

	"andescryptokit/engine"
	"andescryptokit/errno"
	"andescryptokit/types"
)

// UserBased
type UserBased struct {
}

// NewEngineTransCipher 创建传输加密引擎。
//  @param cipherMode 工作模式：加密、解密。
//  @param cipherMaterial 加密材料。如果为空，将会从reader读取，如果不为空但解析加密材料失败，则返回错误。
//  @param reader 待处理的数据流。
//  @return *engine.EngineTransCipher 传输加密引擎对象。
//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
func (u *UserBased)NewEngineTransCipher(cipherMode types.CipherMode, cipherMaterial []byte, reader io.Reader) (*engine.EngineTransCipher, *errno.Errno){
		return engine.NewEngineTransCipher(cipherMode, cipherMaterial, reader, nil, nil, false)
}

// NewEngineFileCipher 创建文件加密引擎。
//  @param cipherMode 工作模式：加密、解密。
//  @param cipherMaterial 加密材料。如果为空，将重新向KMS申请DEK，如果不为空但解析加密材料失败，则返回错误。
//  @param reader 待处理的数据流。
//  @param blockSize 数据分组长度，必须为16的倍数。数据将按设定的分组长度进行分组加密，用户随机解密的最新分段即为该分组大小。
//  @return *engine.EngineFileCipher 文件加密引擎对象。
//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
func (u *UserBased) NewEngineFileCipher(cipherMode types.CipherMode, cipherMaterial []byte, reader io.Reader, blockSize uint64) (*engine.EngineFileCipher, *errno.Errno) {
	return engine.NewEngineFileCipher(cipherMode, nil, reader, blockSize, nil)
}

func (s *UserBased) NewEngineAesGCMCipher(cipherMaterial []byte) (*engine.EngineAesGCMCipher, *errno.Errno) {
	return engine.NewEngineAesGCMCipher(nil)
}
