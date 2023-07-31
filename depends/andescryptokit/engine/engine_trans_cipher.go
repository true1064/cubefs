/*
Copyright OPPO Corp. All Rights Reserved.
*/

// package engine is used to define all interfaces exposed to users, and different
// encryption schemes should implement these interfaces.
package engine

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"

	"andescryptokit/errno"
	"andescryptokit/types"
)

// EngineTransCipher 流式传输加（解）密引擎。
type EngineTransCipher struct {
	// reader 数据输入流。
	reader io.Reader

	// cipherMode 加密模式。
	cipherMode types.CipherMode

	// cipherMaterial 加密材料。
	cipherMaterialReader io.Reader

	// stream 加密流。
	stream cipher.Stream

	// needHmac 是否需要开启完整性校验值计算。
	needHmac bool

	// hashMac 完整性校验值计算引擎。
	hashMac hash.Hash
}

// NewEngineTransCipher 创建传输加密引擎。
//  @param cipherMode 工作模式：加密、解密。
//  @param cipherMaterial 加密材料。如果为空，将会从reader读取，如果不为空但解析加密材料失败，则返回错误。
//  @param reader 待处理的数据流。
//  @return *engine.EngineTransCipher 传输加密引擎对象。
//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
func NewEngineTransCipher(cipherMode types.CipherMode, cipherMaterial []byte, reader io.Reader, key, iv []byte, needHmac bool) (*EngineTransCipher, *errno.Errno) {
	if len(key) != AES_256_BIT {
		return nil, errno.TransCipherKeyLengthError.Append(fmt.Sprintf(" key len %d.", len(key)))
	}

	if len(iv) != AES_128_BIT {
		return nil, errno.TransCipherIVLengthError.Append(fmt.Sprintf(" iv len %d.", len(iv)))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errno.TransCipherInternalError
	}

	return &EngineTransCipher{
		stream:               cipher.NewCTR(block, iv), // no error returned
		needHmac:             needHmac,
		hashMac:              hmac.New(sha256.New, []byte(key)), // no error returned
		reader:               reader,
		cipherMode:           cipherMode,
		cipherMaterialReader: bytes.NewReader(cipherMaterial),
	}, errno.OK
}

// Read 读取一部分明（密）文数据。加密模式下，默认会先读取加密材料，除非调用GetCipherMaterial()先读走。
//  @receiver e
//  @param p 存储读取的明（密）文数据。
//  @return int 成功读取到明（密）文数据的长度。
//  @return error 返回错误信息或文件结束标示EOF。
func (e *EngineTransCipher) Read(p []byte) (int, error) {
	// 加密模式下，如果加密材料没有被读走，则先返回加密材料。
	if e.cipherMode == types.ENCRYPT_MODE && e.cipherMaterialReader != nil {
		// 不能返回err，因为还需要继续读其他reader
		n, _ := e.cipherMaterialReader.Read(p)
		if n > 0 {
			return n, nil
		}
	}

	// 没有数据直接报错
	if e.reader == nil {
		return 0, fmt.Errorf("reader is nil.")
	}

	// 读取数据
	data := make([]byte, len(p))
	n, err := e.reader.Read(data)
	if n > 0 {
		switch e.cipherMode {
		case types.ENCRYPT_MODE:
			cipherErr := e.encrypt(p, data[:n])
			if cipherErr != nil {
				return 0, cipherErr
			}

		case types.DECRYPT_MODE:
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

// GetHashMac 获取加解密的密文校验值，它应当在数据加解密完成后调用。
//  @receiver e
//  @return []byte 完整性校验值。
//  @return *errno.Errno 如果出错，返回错误原因。
func (e *EngineTransCipher) GetHashMac() ([]byte, *errno.Errno) {
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
func (e *EngineTransCipher) Verify(hashmac []byte) (bool, *errno.Errno) {
	if !e.needHmac {
		return false, errno.TransCipherHmacOffError
	}

	return hmac.Equal(hashmac, e.hashMac.Sum(nil)), errno.OK
}

// GetCipherMaterial 获取加密材料。
//  @receiver e
//  @return []byte 加密材料。
func (e *EngineTransCipher) GetCipherMaterial() []byte {
	if e.cipherMaterialReader == nil {
		return nil
	}

	cipherMaterial, _ := io.ReadAll(e.cipherMaterialReader)
	return cipherMaterial
}

// encrypt 将一段明文数据plaintext加密成密文，同时将密文存储至ciphertext。
//
//  @receiver e
//  @param ciphertext 接收加密成功的密文数据，它的长度不应小于明文长度。
//  @param plaintext 待加密的用户数据明文。
//  @return error 如果出错，返回错误原因。
func (e *EngineTransCipher) encrypt(ciphertext, plaintext []byte) error {
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
func (e *EngineTransCipher) decrypt(plaintext, ciphertext []byte) error {
	e.stream.XORKeyStream(plaintext, ciphertext)

	if e.needHmac {
		_, err := e.hashMac.Write(ciphertext)
		if err != nil {
			return err
		}
	}

	return nil
}
