/*
Copyright OPPO Corp. All Rights Reserved.
*/

// package engine is used to define all interfaces exposed to users, and different
// encryption schemes should implement these interfaces.
package engine

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"hash"

	"oppo.com/andes-crypto/kit/server/errno"
)

// TransCipher 用于用户数据传输的二次加密。使用算法默认为CTR流式加密，支持大文件的加密。
type EngineTransCipher struct {
	stream cipher.Stream

	needHmac bool

	hashMac hash.Hash
}

// NewEngineTransCipher 创建传输加密引擎。
//
//	@param cipherKey 传输加密密钥的密文，它在终端侧创建后由AndesCryptoKit的公钥加密再经过base64编码，通过HTTP请求的头部字段"Transfer-Key"传输到云端。
//	@param cipherIv 传输加密IV的密文，它在终端侧创建后由AndesCryptoKit的公钥加密再经过base64编码，通过HTTP请求的头部字段"Transfer-IV"传输到云端。
//	@return *engine.EngineTransCipher
//	@return error 分配引擎出错则返回错误原因，否则返回空。
func NewEngineTransCipher(key, iv []byte, needHmac bool) (*EngineTransCipher, *errno.Errno) {
	if len(key) != AES_256_BIT {
		return nil, errno.TransCipherKeyLengthError
	}

	if len(iv) != AES_128_BIT {
		return nil, errno.TransCipherIVLengthError
	}

	if block, err := aes.NewCipher(key); err == nil {
		return &EngineTransCipher{
			stream:   cipher.NewCTR(block, iv),
			needHmac: needHmac,
			hashMac:  hmac.New(sha256.New, []byte(key)),
		}, errno.OK
	}

	return nil, errno.TransCipherInternalError
}

// Encrypt 将一段明文数据plaintext加密成密文，同时将密文存储至ciphertext。
//
//	@receiver e
//	@param ciphertext 接收加密成功的密文数据，它的长度不应小于明文长度。
//	@param plaintext 待加密的用户数据明文。
//	@return *errno.Errno 如果出错，返回出错错误码以及详细信息。
func (e *EngineTransCipher) Encrypt(ciphertext, plaintext []byte) *errno.Errno {
	if len(ciphertext) < len(plaintext) {
		return errno.TransCipherCiphertextLenError
	}

	e.stream.XORKeyStream(ciphertext, plaintext)

	if e.needHmac {
		n, err := e.hashMac.Write(ciphertext)
		if n != len(ciphertext) {
			return errno.TransCipherHmacWriteError
		} else if err != nil {
			return errno.TransCipherHmacWriteClosedWithError.Append(err.Error())
		}
	}

	return errno.OK
}

// Decrypt 将一段密文数据ciphertext解密成明文，同时将明文存储至plaintext。
//
//	@receiver e
//	@param plaintext 接收解密成功的明文数据，它的长度不应小于密文长度。
//	@param ciphertext 待解密的用户数据密文。
//	@return *errno.Errno 如果出错，返回出错错误码以及详细信息。
func (e *EngineTransCipher) Decrypt(plaintext, ciphertext []byte) *errno.Errno {
	if len(plaintext) < len(ciphertext) {
		return errno.TransCipherPlaintextLenError
	}

	e.stream.XORKeyStream(plaintext, ciphertext)

	if e.needHmac {
		n, err := e.hashMac.Write(ciphertext)
		if n != len(ciphertext) {
			return errno.TransCipherHmacWriteError
		} else if err != nil {
			return errno.TransCipherHmacWriteClosedWithError.Append(err.Error())
		}
	}

	return errno.OK
}

// GetHashMac 获取加（解）密的密文校验值，它应当在数据加（解）密完成后调用。
//
//	@receiver e
//	@return []byte 完整性校验值。
//	@return *errno.Errno 如果出错，返回出错错误码以及详细信息。
func (e *EngineTransCipher) GetHashMac() ([]byte, *errno.Errno) {
	if !e.needHmac {
		return nil, errno.TransCipherHmacOffError
	}

	return e.hashMac.Sum(nil), errno.OK
}

// Verify 在不泄漏时序信息的情况下比较两个HASH MAC值是否相等。
//
//	@receiver e
//	@param hash 传输的密文消息校验值。
//	@return bool 如果相同则返回真，否则返回假。
//	@return *errno.Errno 如果出错，返回出错错误码以及详细信息。
func (e *EngineTransCipher) Verify(hashmac []byte) (bool, *errno.Errno) {
	if !e.needHmac {
		return false, errno.TransCipherHmacOffError
	}

	return hmac.Equal(hashmac, e.hashMac.Sum(nil)), errno.OK
}
