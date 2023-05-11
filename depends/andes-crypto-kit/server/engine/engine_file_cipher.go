/*
Copyright OPPO Corp. All Rights Reserved.
*/

// package engine is used to define all interfaces exposed to users, and different
// encryption schemes should implement these interfaces.
package engine

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"

	"golang.org/x/crypto/xts"

	"oppo.com/andes-crypto/kit/server/errno"
)

const (
	AES_128_BIT = 16
	AES_256_BIT = 32
	AES_512_BIT = 64
)

// EngineFileCipher 文件加密引擎，加密模式为AES-256-XTS，明文与密文长度比例为1:1。注意，一个对象只负责单一场景，即
// 创建对象后，要么只加密，要么只解密，不能用同一个对象即加密又解密。并且待加密的数据长度必须大于等于一个分组长度。
type EngineFileCipher struct {

	// cipher
	cipher *xts.Cipher

	// plainKey 加密密钥的明文
	plainKey []byte

	// cipherKey 加密密钥的密文
	cipherKey []byte

	// blockSize 加密数据块长度，它将指导XTS加密算法的序号section number。
	blockSize uint64

	// sectorNum 加密数据块的序号
	sectorNum uint64

	// reserve 缓存数据，当用户传进来的数据不够一个分组长度时，需要先暂存在这里，待下一次数据传进来时再次加密。
	dataReserved []byte

	// lastBlockReserved  最后一个分组缓存，每次加密完成后都将被更新，它将用于最后分组小于分组长度时的窃取补全操作。
	lastBlockReserved []byte
}

// NewEngineFileCipher 创建文件存储加密引擎。
//
//	@param plainKey 加密密钥的明文。
//	@param cipherKey 加密密钥的密文。
//	@param blockSize 分组长度，必须为16的整数倍。
//	@return *EngineFileCipher 文件加密对象。
//	@return []byte 加密密钥的密文，用户应当保存该密钥。
//	@return error
func NewEngineFileCipher(plainKey, cipherKey []byte, blockSize uint64) (*EngineFileCipher, []byte, *errno.Errno) {
	if len(plainKey) != AES_256_BIT && len(plainKey) != AES_512_BIT {
		return nil, nil, errno.FileCipherKeyLengthError.Append(fmt.Sprintf("key length:%d", len(plainKey)))
	}

	if blockSize%16 != 0 {
		return nil, nil, errno.FileCipherBlockSizeError.Append(fmt.Sprintf("block size:%d", blockSize))
	}

	var err error
	var cipher *xts.Cipher

	if cipher, err = xts.NewCipher(aes.NewCipher, plainKey); err != nil {
		return nil, nil, errno.FileCipherNewXtsCipherError.Append(err.Error())
	}

	return &EngineFileCipher{
		cipher:    cipher,
		plainKey:  plainKey,
		cipherKey: cipherKey,
		blockSize: blockSize,
		sectorNum: 0,
	}, cipherKey, errno.OK
}

// prepareBlockSizeData 将传进来的数据按设定的分组大小分组。
//
//	@receiver e
//	@param data 待加密/解密的数据。
//	@return []byte 分组长度的数据。
//	@return error 如果出错，返回错误原因。
func (e *EngineFileCipher) prepareBlockSizeData(data []byte) ([]byte, *errno.Errno) {
	// if 数据长度比设定的block长度大，则不加密数据。
	if len(data) > int(e.blockSize) {
		return nil, errno.FileCipherFileModePlaintextLengthError.Append(fmt.Sprintf("plaintext len:%d, block size:%d", len(data), e.blockSize))
	}

	var blockSizeData []byte

	e.dataReserved = append(e.dataReserved, data...)
	if len(e.dataReserved) >= int(e.blockSize) {
		blockSizeData = e.dataReserved[:e.blockSize]
		e.dataReserved = e.dataReserved[e.blockSize:]
	}

	return blockSizeData, errno.OK
}

// encryptCTR 使用AES-256-CTR算法加密一段数据。
//  @receiver e
//  @param plaintext 待加密的明文数据。
//  @return []byte 加密成功后的密文数据。
//  @return *errno.Errno 如果出错，返回错误码以及详细信息。
func (e *EngineFileCipher) encryptCTR(plaintext []byte) ([]byte, *errno.Errno) {
	block, err := aes.NewCipher(e.plainKey)
	if err != nil {
		return nil, errno.FileCipherFileModeTotalBlockLessThanOneError
	}

	stream := cipher.NewCTR(block, e.plainKey[:block.BlockSize()])
	ciphertext := make([]byte, len(plaintext))
	stream.XORKeyStream(ciphertext, plaintext)

	return ciphertext, errno.OK
}

// decryptCTR 使用AES-256-CTR算法解密一段数据。
//  @receiver e
//  @param ciphertext 待解密的密文数据。
//  @return []byte 解密成功后的明文数据。
//  @return *errno.Errno 如果出错，返回错误码以及详细信息。
func (e *EngineFileCipher) decryptCTR(ciphertext []byte) ([]byte, *errno.Errno) {
	block, err := aes.NewCipher(e.plainKey)
	if err != nil {
		return nil, errno.FileCipherFileModeTotalBlockLessThanOneError
	}
	stream := cipher.NewCTR(block, e.plainKey[:block.BlockSize()])
	plaintext := make([]byte, len(ciphertext))
	stream.XORKeyStream(plaintext, ciphertext)

	return plaintext, errno.OK
}

// EncryptBlock 加密数据，待加密数据长度必须等于创建对象时设置的分组长度。
//
//	@receiver e
//	@param plaintext 待加密的明文数据。
//	@param sectorNum 待加密的明文数据的分组序号。
//	@return []byte 加密后的数据密文。
//	@return *errno.Errno 如果出错，返回出错错误码以及详细信息。
func (e *EngineFileCipher) EncryptBlock(plaintext []byte, sectorNum uint64) ([]byte, *errno.Errno) {
	if len(plaintext) != int(e.blockSize) {
		return nil, errno.FileCipherBlockModePlaintextLengthError.Append(fmt.Sprintf("plaintext len:%d, block size:%d", len(plaintext), e.blockSize))
	}

	// 加密数据，同时将分组序号递增
	ciphertext := make([]byte, len(plaintext))
	e.cipher.Encrypt(ciphertext, plaintext, sectorNum)

	return ciphertext, errno.OK
}

// EncryptFile 加密一个文件，通过多次调用达到对一个文件的完整加密。
//
//	@receiver e
//	@param plaintext 待加密的明文数据。
//	@return []byte 加密后的密文数据。
//	@return *errno.Errno 如果出错，返回出错错误码以及详细信息。
func (e *EngineFileCipher) EncryptFile(plaintext []byte) ([]byte, *errno.Errno) {
	// 将明文数据以设定的分组长度进行分组
	blockSizeData, err := e.prepareBlockSizeData(plaintext)
	if err != errno.OK {
		return nil, err
	}

	// 待加密数据的长度可能是小于block size的，此时只需将数据缓存。
	if len(blockSizeData) == 0 {
		return nil, errno.OK
	}

	// 加密数据，同时将分组序号递增
	ciphertextTemp := make([]byte, len(blockSizeData))
	e.cipher.Encrypt(ciphertextTemp, blockSizeData, e.sectorNum)
	e.sectorNum += 1

	// 将前一个缓存的密文返回给用户。
	ciphertext := e.lastBlockReserved
	// 缓存当前密文分组，当最后的待加密数据长度小于分组长度时，需要窃取补位。
	e.lastBlockReserved = ciphertextTemp

	return ciphertext, errno.OK
}

// DecryptBlock 解密数据，待解密数据长度必须等于创建对象时设置的分组长度。
//
//	@receiver e
//	@param ciphertext 待解密的用户数据。
//	@param sectorNum 密文数据的分组序号。
//	@return []byte 解密后的明文数据。
//	@return *errno.Errno 如果出错，返回出错错误码以及详细信息。
func (e *EngineFileCipher) DecryptBlock(ciphertext []byte, sectorNum uint64) ([]byte, *errno.Errno) {
	if len(ciphertext) != int(e.blockSize) {
		return nil, errno.FileCipherBlockModeCiphertextLengthError.Append(fmt.Sprintf("ciphertext len:%d, block size:%d", len(ciphertext), e.blockSize))
	}

	// 解密数据
	plaintext := make([]byte, len(ciphertext))
	e.cipher.Decrypt(plaintext, ciphertext, sectorNum)

	return plaintext, errno.OK
}

// DecryptFile 解密一个文件，通过多次调用达到对一个文件的完整解密。
//
//	@receiver e
//	@param ciphertext 待解密的密文数据。
//	@return []byte 解密后的数据明文。
//	@return *errno.Errno 如果出错，返回出错错误码以及详细信息。
func (e *EngineFileCipher) DecryptFile(ciphertext []byte) ([]byte, *errno.Errno) {
	// 将明文数据以设定的分组长度进行分组
	blockSizeData, err := e.prepareBlockSizeData(ciphertext)
	if err != errno.OK {
		return nil, err
	}

	// 缓存的数据还未达到一个分组大小，不做任何处理
	if len(blockSizeData) == 0 {
		return nil, errno.OK
	}

	var plaintext []byte
	if len(e.lastBlockReserved) > 0 {
		plaintext = make([]byte, len(e.lastBlockReserved))
		// 解密前一个缓存的密文分组
		e.cipher.Decrypt(plaintext, e.lastBlockReserved, e.sectorNum)
		e.sectorNum += 1
	}

	// 缓存当前密文分组，以满足可能的最后一个分组窃取补位。
	e.lastBlockReserved = blockSizeData

	return plaintext, errno.OK
}

// FinalEncrypt 文件加密的最后处理，它将对最后不足一个分组长度的数据分组进行窃取补位。如果整个数据长度不足一个分组长度，那么将会切换加密算法为AES-256-CTR。
//
//	@receiver e
//	@return []byte 最后分组数据的密文。
//	@return *errno.Errno 如果出错，返回出错错误码以及详细信息。
func (e *EngineFileCipher) FinalEncryptFile() ([]byte, *errno.Errno) {
	dataReservedLen := len(e.dataReserved)
	if e.lastBlockReserved == nil {
		return e.encryptCTR(e.dataReserved)
		// return nil, errno.FileCipherFileModeTotalBlockLessThanOneError.Append(fmt.Sprintf("data len:%d, block size:%d", dataReservedLen, e.blockSize))
	}

	// 最后一个分组的长度刚好等于一个分组长度，这种不需要窃取补位
	if len(e.dataReserved) == 0 {
		return e.lastBlockReserved, errno.OK
	} else { // 最后一个分组窃取补位。
		e.dataReserved = append(e.dataReserved, e.lastBlockReserved[dataReservedLen:]...)
		// 加密数据，同时将分组序号递增
		ciphertext := make([]byte, len(e.dataReserved))
		e.cipher.Encrypt(ciphertext, e.dataReserved, e.sectorNum)
		e.sectorNum += 1

		// 将缓存的分组以及最后一个分组密文一起返回。
		ciphertext = append(ciphertext, e.lastBlockReserved[:dataReservedLen]...)
		return ciphertext, errno.OK
	}
}

// FinalDecryptFile 文件解密的最后处理，它将对最后不足一个分组长度的数据分组进行窃取补位。
//
//	@receiver e
//	@return []byte 解密后的数据明文。
//	@return *errno.Errno 如果出错，返回出错错误码以及详细信息。
func (e *EngineFileCipher) FinalDecryptFile() ([]byte, *errno.Errno) {
	var plaintext []byte
	dataReservedLen := len(e.dataReserved)

	if e.lastBlockReserved == nil {
		return e.decryptCTR(e.dataReserved)
	}

	// 最后一个分组的长度刚好等于一个分组长度，这种不需要窃取补位
	if len(e.dataReserved) == 0 {
		plaintext = make([]byte, len(e.lastBlockReserved))
		e.cipher.Decrypt(plaintext, e.lastBlockReserved, e.sectorNum)
	} else { // 最后一个分组窃取补位。

		// 解密倒数第二个密文分组
		plaintextTemp := make([]byte, len(e.lastBlockReserved))
		e.cipher.Decrypt(plaintextTemp, e.lastBlockReserved, e.sectorNum+1)

		// 窃取补齐最后一个密文分组，然后解密
		e.dataReserved = append(e.dataReserved, plaintextTemp[len(e.dataReserved):]...)
		plaintext = make([]byte, len(e.dataReserved))
		e.cipher.Decrypt(plaintext, e.dataReserved, e.sectorNum)

		// 将缓存的明文以及最后解密的明文一起返回。
		plaintext = append(plaintext, plaintextTemp[:dataReservedLen]...)
	}

	return plaintext, errno.OK
}
