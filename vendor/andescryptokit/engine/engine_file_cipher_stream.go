/*
Copyright OPPO Corp. All Rights Reserved.
*/

// package engine is used to define all interfaces exposed to users, and different
// encryption schemes should implement these interfaces.
package engine

import (
	"andescryptokit/errno"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"io"

	"golang.org/x/crypto/xts"
)

// EngineFileCipher 文件加密引擎，加密模式为AES-256-XTS，明文与密文长度比例为1:1。注意，一个对象只负责单一场景，即
// 创建对象后，要么只加密，要么只解密，不能用同一个对象即加密又解密。并且待加密的数据长度必须大于等于一个分组长度。
type EngineFileCipherStream struct {
	// reader 数据输入流。
	reader io.Reader

	// finalReader 加解密数据最后2个block数据缓存流
	finalReader io.Reader

	// cipherMode 加密模式。
	cipherMode CipherMode

	// cipher AES-XTS加密引擎
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

// NewEngineFileCipherStream 创建文件存储加密流式引擎。
//
//  @param plainKey 加密密钥的明文。
//  @param cipherKey 加密密钥的密文。
//  @param blockSize 分组长度，必须为16的整数倍。
//  @param cipherMode 工作模式：加密或解密。
//  @param reader 待处理的数据流。
//  @return *EngineFileCipherStream
//  @return []byte 加密密钥的密文，用户应当保存该密钥。
//  @return *errno.Errno 如果出错返回错误原因。
func NewEngineFileCipherStream(plainKey, cipherKey []byte, blockSize uint64, cipherMode CipherMode, reader io.Reader) (*EngineFileCipherStream, []byte, *errno.Errno) {
	if len(plainKey) != AES_256_BIT && len(plainKey) != AES_512_BIT {
		return nil, nil, errno.FileCipherKeyLengthError.Append(fmt.Sprintf("key length:%d", len(plainKey)))
	}

	if blockSize%16 != 0 || blockSize == 0 {
		return nil, nil, errno.FileCipherBlockSizeError.Append(fmt.Sprintf("block size:%d", blockSize))
	}

	if reader == nil {
		return nil, nil, errno.FileCipherStreamModeNilStreamError
	}

	var err error
	var cipher *xts.Cipher

	if cipher, err = xts.NewCipher(aes.NewCipher, plainKey); err != nil {
		return nil, nil, errno.FileCipherNewXtsCipherError.Append(err.Error())
	}

	return &EngineFileCipherStream{
		cipher:     cipher,
		plainKey:   plainKey,
		cipherKey:  cipherKey,
		blockSize:  blockSize,
		sectorNum:  0,
		reader:     reader,
		cipherMode: cipherMode,
	}, cipherKey, errno.OK
}

// Read 从输入流中读取一部分数据，长度不得小于一个分组长度。
//  @receiver e
//  @param p 读取的数据。
//  @return int 成功读取的数据的长度。
//  @return error 返回错误信息或在文件结束标示EOF。
func (e *EngineFileCipherStream) Read(p []byte) (int, error) {
	if len(p) < int(e.blockSize) {
		return 0, fmt.Errorf("data length[%d] is smaller than block size[%d].", len(p), e.blockSize)
	}

	// 数据已经完成加解密，需要获取最后缓存的block数据。
	if e.finalReader == nil {
		data := make([]byte, e.blockSize)
		n, err := e.reader.Read(data)
		if n > 0 {
			switch e.cipherMode {
			case ENCRYPT_MODE:
				n = e.encrypt(p, data[:n])
			case DECRYPT_MODE:
				n = e.decrypt(p, data[:n])
			default:
				return 0, fmt.Errorf("Error with cipher mode:%d", e.cipherMode)
			}
		}

		if err == io.EOF {
			switch e.cipherMode {
			case ENCRYPT_MODE:
				data, finalErr := e.finalEncryptFile()
				if finalErr == nil {
					e.finalReader = bytes.NewReader(data)
				} else {
					err = finalErr
				}

			case DECRYPT_MODE:
				data, finalErr := e.finalDecryptFile()
				if finalErr == nil {
					e.finalReader = bytes.NewReader(data)
				} else {
					err = finalErr
				}

			default:
				return 0, fmt.Errorf("Error with cipher mode:%d", e.cipherMode)
			}

			return e.finalReader.Read(p)
		}

		return n, err
	} else {
		return e.finalReader.Read(p)
	}
}

// EncryptBlock 以分组块的形式加密一段数据，如果数据小于一个block size，认为整个文件的大小是block size；
// 如果需要加密最后一个不足一个分组大小的数据，则应当将前一个分组也一并传入；其他情况应以分组传入数据。
//
//  @receiver e
//  @param ciphertext 存储加密后的数据。
//  @param plaintext 待加密的明文数据。
//  @param sectorNum 待加密的明文数据的分组序号，它是这段数据的起始序号。
//  @return *errno.Errno 如果出错，返回出错错误码以及详细信息。
func (e *EngineFileCipherStream) EncryptBlock(ciphertext, plaintext []byte, sectorNum uint64) *errno.Errno {
	if sectorNum < 0 {
		return errno.FileCipherBlockModeSectorNumError
	}

	plaintextLen := len(plaintext)
	if len(ciphertext) < plaintextLen {
		return errno.FileCipherBlockModeCiphertextLengthError
	}

	// 刚好一个分组大小
	if plaintextLen == int(e.blockSize) {
		e.cipher.Encrypt(ciphertext, plaintext, sectorNum)
		return errno.OK
	}

	// 不足一个块，认为数据总长度只有这么长，因此切换为AES-CTR
	if plaintextLen < int(e.blockSize) {
		data, err := e.encryptCTR(plaintext)
		if err != nil {
			return errno.FileCipherNewXtsCipherError.Append(err.Error())
		}

		copy(ciphertext, data)
		return errno.OK
	}

	// 大于一个块，需要分组加密
	start := 0
	for start < plaintextLen {
		end := start + int(e.blockSize)
		if end <= plaintextLen {
			e.cipher.Encrypt(ciphertext[start:end], plaintext[start:end], sectorNum)
		} else {
			// 最后一个分组不足一个块
			lastPlaintextLen := plaintextLen - start
			lastPlaintextNeedLen := e.blockSize - uint64(lastPlaintextLen)

			// 加密最后一个分组
			lastPlaintextBlock := append(plaintext[start:plaintextLen], ciphertext[uint64(start)-lastPlaintextNeedLen:start]...)
			lastCiphertextBlock := make([]byte, e.blockSize)
			e.cipher.Encrypt(lastCiphertextBlock, lastPlaintextBlock, sectorNum)

			// 密文调整1：倒数第2个密文分组的部分数据为整个密文的尾部
			from := start - int(e.blockSize)
			copy(ciphertext[start:], ciphertext[from:(from+lastPlaintextLen)])
			// 密文调整2：倒数第1密文分组移动到倒数第2个分组
			copy(ciphertext[from:start], lastCiphertextBlock)
		}

		start += int(e.blockSize)
		sectorNum += 1
	}

	return errno.OK
}

// DecryptBlock 以分组块的形式解密一段数据，如果数据小于一个block size，认为整个文件的大小是block size；
// 如果需要解密最后一个不足一个分组大小的数据，则应当将前一个分组也一并传入；其他情况应以分组传入数据。
//
//  @receiver e
//  @param plaintext 解密后的明文数据。
//  @param ciphertext 待解密的用户数据。
//  @param sectorNum 密文数据的分组序号，它是这段数据的起始序号。
//  @return *errno.Errno 如果出错，返回出错错误码以及详细信息。
func (e *EngineFileCipherStream) DecryptBlock(plaintext, ciphertext []byte, sectorNum uint64) *errno.Errno {
	if sectorNum < 0 {
		return errno.FileCipherBlockModeSectorNumError
	}

	ciphertextLen := len(ciphertext)
	if len(plaintext) < ciphertextLen {
		return errno.FileCipherBlockModePlaintextLengthError
	}

	// 刚好一个分组大小
	if ciphertextLen == int(e.blockSize) {
		e.cipher.Decrypt(plaintext, ciphertext, sectorNum)
		return errno.OK
	}

	// 不足一个块，认为数据总长度只有这么长，因此切换为AES-CTR
	if ciphertextLen < int(e.blockSize) {
		data, err := e.decryptCTR(ciphertext)
		if err != nil {
			return errno.FileCipherNewXtsCipherError.Append(err.Error())
		}

		copy(plaintext, data)
		return errno.OK
	}

	// 大于一个块，需要分组解密
	start := 0
	for start < ciphertextLen {
		end := start + int(e.blockSize)
		if end <= ciphertextLen {
			e.cipher.Decrypt(plaintext[start:end], ciphertext[start:end], sectorNum)
		} else {
			// 最后一个分组不足一个块
			lastCiphertextLen := ciphertextLen - start
			lastCiphertextNeedLen := e.blockSize - uint64(lastCiphertextLen)

			// 倒数第2个分组的起始序号
			from := start - int(e.blockSize)

			// 重新解密倒数第2个块，其sector num需要调整下
			e.cipher.Decrypt(plaintext[from:start], ciphertext[from:start], sectorNum)

			// 解密最后一个分组
			lastCiphertextBlock := append(ciphertext[start:ciphertextLen], plaintext[uint64(start)-lastCiphertextNeedLen:start]...)
			lastPlaintextBlock := make([]byte, e.blockSize)
			e.cipher.Decrypt(lastPlaintextBlock, lastCiphertextBlock, sectorNum-1)

			// 明文调整1：倒数第2个明文分组的部分数据为整个明文的尾部
			copy(plaintext[start:], plaintext[from:(from+lastCiphertextLen)])
			// 明文调整2：倒数第1明文分组移动到倒数第2个分组
			copy(plaintext[from:start], lastPlaintextBlock)
		}

		start += int(e.blockSize)
		sectorNum += 1
	}
	return errno.OK
}

// encryptCTR 使用AES-256-CTR算法加密一段数据。
//  @receiver e
//  @param plaintext 待加密的明文数据。
//  @return []byte 加密成功后的密文数据。
func (e *EngineFileCipherStream) encryptCTR(plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(e.plainKey)
	if err != nil {
		return nil, err
	}

	stream := cipher.NewCTR(block, e.plainKey[:block.BlockSize()])
	ciphertext := make([]byte, len(plaintext))
	stream.XORKeyStream(ciphertext, plaintext)

	return ciphertext, nil
}

// decryptCTR 使用AES-256-CTR算法解密一段数据。
//  @receiver e
//  @param ciphertext 待解密的密文数据。
//  @return []byte 解密成功后的明文数据。
func (e *EngineFileCipherStream) decryptCTR(ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(e.plainKey)
	if err != nil {
		return nil, err
	}

	stream := cipher.NewCTR(block, e.plainKey[:block.BlockSize()])
	plaintext := make([]byte, len(ciphertext))
	stream.XORKeyStream(plaintext, ciphertext)

	return plaintext, nil
}

// encrypt 加密一个文件，通过多次调用达到对一个文件的完整加密。
//
//  @receiver e
//  @param plaintext 待加密的明文数据。
//  @return []byte 加密后的密文数据。
func (e *EngineFileCipherStream) encrypt(ciphertext, plaintext []byte) int {
	ciphretextLen := 0

	// 将明文数据以设定的分组长度进行分组
	blockSizeData, err := e.prepareBlockSizeData(plaintext)
	if err != errno.OK {
		return 0
	}

	// 待加密数据的长度可能是小于block size的，此时只需将数据缓存。
	if len(blockSizeData) == 0 {
		return 0
	}

	// 加密数据，同时将分组序号递增
	ciphertextTemp := make([]byte, len(blockSizeData))
	e.cipher.Encrypt(ciphertextTemp, blockSizeData, e.sectorNum)
	e.sectorNum += 1

	// 将前一个缓存的密文返回给用户。
	if e.lastBlockReserved != nil {
		copy(ciphertext, e.lastBlockReserved)
		ciphretextLen = int(e.blockSize)
	}

	// 缓存当前密文分组，当最后的待加密数据长度小于分组长度时，需要窃取补位。
	e.lastBlockReserved = ciphertextTemp

	return ciphretextLen
}

// decrypt 解密一个文件，通过多次调用达到对一个文件的完整解密。
//
//  @receiver e
//  @param ciphertext 待解密的密文数据。
//  @return []byte 解密后的数据明文。
func (e *EngineFileCipherStream) decrypt(plaintext, ciphertext []byte) int {
	plaintextLen := 0

	// 将明文数据以设定的分组长度进行分组
	blockSizeData, err := e.prepareBlockSizeData(ciphertext)
	if err != errno.OK {
		return 0
	}

	// 缓存的数据还未达到一个分组大小，不做任何处理
	if len(blockSizeData) == 0 {
		return 0
	}

	if len(e.lastBlockReserved) > 0 {
		// 解密前一个缓存的密文分组
		e.cipher.Decrypt(plaintext, e.lastBlockReserved, e.sectorNum)
		e.sectorNum += 1
		plaintextLen = int(e.blockSize)
	}

	// 缓存当前密文分组，以满足可能的最后一个分组窃取补位。
	e.lastBlockReserved = blockSizeData

	return plaintextLen
}

// finalEncryptFile 文件加密的最后处理，它将对最后不足一个分组长度的数据分组进行窃取补位。如果整个数据长度不足一个分组长度，那么将会切换加密算法为AES-256-CTR。
//
//  @receiver e
//  @return []byte 最后分组数据的密文。
func (e *EngineFileCipherStream) finalEncryptFile() ([]byte, error) {
	dataReservedLen := len(e.dataReserved)
	if e.lastBlockReserved == nil {
		return e.encryptCTR(e.dataReserved)
		// return nil, errno.FileCipherFileModeTotalBlockLessThanOneError.Append(fmt.Sprintf("data len:%d, block size:%d", dataReservedLen, e.blockSize))
	}

	// 最后一个分组的长度刚好等于一个分组长度，这种不需要窃取补位
	if len(e.dataReserved) == 0 {
		return e.lastBlockReserved, nil
	} else { // 最后一个分组窃取补位。
		e.dataReserved = append(e.dataReserved, e.lastBlockReserved[dataReservedLen:]...)
		// 加密数据，同时将分组序号递增
		ciphertext := make([]byte, len(e.dataReserved))
		e.cipher.Encrypt(ciphertext, e.dataReserved, e.sectorNum)
		e.sectorNum += 1

		// 将缓存的分组以及最后一个分组密文一起返回。
		ciphertext = append(ciphertext, e.lastBlockReserved[:dataReservedLen]...)
		return ciphertext, nil
	}
}

// finalDecryptFile 文件解密的最后处理，它将对最后不足一个分组长度的数据分组进行窃取补位。
//
//  @receiver e
//  @return []byte 解密后的数据明文。
func (e *EngineFileCipherStream) finalDecryptFile() ([]byte, error) {
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

	return plaintext, nil
}

// prepareBlockSizeData 将传进来的数据按设定的分组大小分组。
//
//  @receiver e
//  @param data 待加密/解密的数据。
//  @return []byte 分组长度的数据。
//  @return *errno.Errno
func (e *EngineFileCipherStream) prepareBlockSizeData(data []byte) ([]byte, *errno.Errno) {
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
