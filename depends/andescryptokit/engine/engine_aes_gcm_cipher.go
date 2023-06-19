/*
Copyright OPPO Corp. All Rights Reserved.
*/

// package engine is used to define all interfaces exposed to users, and different
// encryption schemes should implement these interfaces.
package engine

import (
	"andescryptokit/errno"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

// EngineAesGCMCipher 基于AES-256-GCM加（解）密用户数据。
type EngineAesGCMCipher struct {

	// key 加密密钥明文。
	key []byte
}

// NewEngineAesGCMCipher 创建AES-GCM加密引擎。
//  @param key 加密密钥明文。
//  @return *EngineAesGCMCipher AES-GCM加密引擎对象指针。
//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
func NewEngineAesGCMCipher(key []byte) (*EngineAesGCMCipher, *errno.Errno) {
	if len(key) != AES_256_BIT {
		return nil, errno.EngineAes256GcmKeySizeError
	}

	return &EngineAesGCMCipher{key: key}, errno.OK
}

// Encrypt 加密一段数据，加密算法为AES-256-GCM。
//  @receiver e
//  @param plaintext 待加密的明文数据。
//  @return []byte 加密成功的密文数据。
//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
func (e *EngineAesGCMCipher) Encrypt(plaintext []byte) ([]byte, *errno.Errno) {
	block, err := aes.NewCipher(e.key)
	if err != nil {
		return nil, errno.TransCipherInternalError.Append(err.Error())
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errno.TransCipherInternalError.Append(err.Error())
	}

	nonce := make([]byte, gcm.NonceSize())
	_, err = io.ReadFull(rand.Reader, nonce)
	if err != nil {
		return nil, errno.TransCipherInternalError.Append(err.Error())
	}

	var ciphertext []byte
	ciphertext = append(ciphertext, nonce...)
	ciphertext = append(ciphertext, gcm.Seal(nil, nonce, plaintext, nil)...)

	return ciphertext, errno.OK
}

// Decrypt 解密一段数据，解密算法为AES-256-GCM。
//  @receiver e
//  @param ciphertext 待解密的密文数据。
//  @return []byte 解密成功的明文数据。
//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
func (e *EngineAesGCMCipher) Decrypt(ciphertext []byte) ([]byte, *errno.Errno) {
	block, err := aes.NewCipher(e.key)
	if err != nil {
		return nil, errno.TransCipherInternalError.Append(err.Error())
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errno.TransCipherInternalError.Append(err.Error())
	}

	if len(ciphertext) < gcm.NonceSize() {
		return nil, errno.TransCipherInternalError.Append(fmt.Sprintf("ciphertext len error:%d", len(ciphertext)))
	}

	nonce := ciphertext[:gcm.NonceSize()]
	out, err := gcm.Open(nil, nonce, ciphertext[gcm.NonceSize():], nil)
	if err != nil {
		return nil, errno.TransCipherInternalError.Append(err.Error())
	}

	return out, errno.OK
}
