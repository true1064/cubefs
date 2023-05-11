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
	"fmt"
	"hash"
	"io"

	"oppo.com/andes-crypto/kit/server/errno"
)

// CipherMode 传输模式
type CipherMode uint16

const (

	// ENCRYPT_MODE 加密模式
	ENCRYPT_MODE CipherMode = 1

	// DECRYPT_MODE 解密模式
	DECRYPT_MODE CipherMode = 2
)

// EngineTransCipherStream 流式传输加（解）密引擎。
type EngineTransCipherStream struct {

	// reader 数据输入流。
	reader io.Reader

	// cipherMode 加密模式。
	cipherMode CipherMode

	// stream 加密流。
	stream cipher.Stream

	// needHmac 是否需要开启完整性校验值计算。
	needHmac bool

	// hashMac 完整性校验值计算引擎。
	hashMac hash.Hash
}

// NewEngineTransCipherStream 创建流式传输加密引擎。
//  @param key 加密密钥。
//  @param iv 初始化向量。
//  @param needHmac 是否开启消息完整性校验。
//  @param cipherMode 模式：加密或者解密。
//  @param reader 读取数据流。
//  @return *EngineTransCipherStream 流式传输加密引擎指针。
//  @return *errno.Errno 如果出错，返回错误原因。
func NewEngineTransCipherStream(key, iv []byte, needHmac bool, cipherMode CipherMode, reader io.Reader) (*EngineTransCipherStream, *errno.Errno) {
	if len(key) != AES_256_BIT {
		return nil, errno.TransCipherKeyLengthError
	}

	if len(iv) != AES_128_BIT {
		return nil, errno.TransCipherIVLengthError
	}

	if block, err := aes.NewCipher(key); err == nil {
		return &EngineTransCipherStream{
			stream:     cipher.NewCTR(block, iv),
			needHmac:   needHmac,
			hashMac:    hmac.New(sha256.New, []byte(key)),
			reader:     reader,
			cipherMode: cipherMode,
		}, errno.OK
	}

	return nil, errno.TransCipherInternalError
}

// Read 从输入流中读取一部分数据。
//  @receiver e
//  @param p 读取的数据写入处。
//  @return int 真正读取的数据的长度。
//  @return error 如果出错，返回错误原因。
func (e *EngineTransCipherStream) Read(p []byte) (int, error) {
	if e.reader == nil {
		return 0, fmt.Errorf("data reader is nil.")
	}

	data := make([]byte, len(p))
	n, err := e.reader.Read(data)
	if n > 0 {
		switch e.cipherMode {
		case ENCRYPT_MODE:
			cipherErr := e.encrypt(p, data[:n])
			if cipherErr != nil {
				return 0, cipherErr
			}

		case DECRYPT_MODE:
			cipherErr := e.decrypt(p, data[:n])
			if cipherErr != nil {
				return 0, cipherErr
			}
		default:
			return 0, fmt.Errorf("Error with cipher mode:%d", e.cipherMode)
		}
	}

	return n, err
}

// SetDataReader 设置数据读取流，加解密将从该流中读取数据。
//  @receiver e
//  @param reader 待加解密的数据流。
func (e *EngineTransCipherStream) SetDataReader(reader io.Reader) {
	e.reader = reader
}

// GetHashMac 获取加解密的密文校验值，它应当在数据加解密完成后调用。
//  @receiver e
//  @return []byte 完整性校验值。
//  @return *errno.Errno 如果出错，返回错误原因。
func (e *EngineTransCipherStream) GetHashMac() ([]byte, *errno.Errno) {
	if !e.needHmac {
		return nil, errno.TransCipherHmacOffError
	}

	return e.hashMac.Sum(nil), errno.OK
}

// Verify 在不泄漏时序信息的情况下比较两个HASH MAC值是否相等。
//
//  @receiver e
//  @param hashmac 传输的密文消息校验值。
//  @return bool 如果相同则返回真，否则返回假。
//  @return *errno.Errno 如果出错，返回出错错误码以及详细信息。
func (e *EngineTransCipherStream) Verify(hashmac []byte) (bool, *errno.Errno) {
	if !e.needHmac {
		return false, errno.TransCipherHmacOffError
	}

	return hmac.Equal(hashmac, e.hashMac.Sum(nil)), errno.OK
}

// encrypt 将一段明文数据plaintext加密成密文，同时将密文存储至ciphertext。
//
//  @receiver e
//  @param ciphertext 接收加密成功的密文数据，它的长度不应小于明文长度。
//  @param plaintext 待加密的用户数据明文。
//  @return error 如果出错，返回错误原因。
func (e *EngineTransCipherStream) encrypt(ciphertext, plaintext []byte) error {
	e.stream.XORKeyStream(ciphertext, plaintext)

	if e.needHmac {
		_, err := e.hashMac.Write(ciphertext[:len(plaintext)])
		if err != nil {
			return err
		}
	}

	return nil
}

// decrypt 将一段密文数据ciphertext解密成明文，同时将明文存储至plaintext。
//
//  @receiver e
//  @param plaintext 接收解密成功的明文数据，它的长度不应小于密文长度。
//  @param ciphertext 待解密的用户数据密文。
//  @return error 如果出错，返回错误原因。
func (e *EngineTransCipherStream) decrypt(plaintext, ciphertext []byte) error {
	e.stream.XORKeyStream(plaintext, ciphertext)

	if e.needHmac {
		_, err := e.hashMac.Write(ciphertext)
		if err != nil {
			return err
		}
	}

	return nil
}
