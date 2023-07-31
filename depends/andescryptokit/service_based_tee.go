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

// ServiceBasedTEE 服务级TEE增强加密方案，端云数据传输基于数字信封，云侧数据存储加密密钥由端侧生成，保证数据只能在数据源设备上解密，从而抵御构造合法请求窃取用户数据攻击。
type ServiceBasedTEE struct {
}

// NewEngineTransCipherStream 创建流式传输加密引擎。
//  @param cipherType 加密类型：加密模式和解密模式。
//  @param reader 数据读取流，如文件流，网络流。
//  @return *engine.EngineTransCipherStream 流式传输加密引擎对象。
//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
func (s *ServiceBasedTEE) NewEngineTransCipher(cipherMode types.CipherMode, reader io.Reader) (*engine.EngineTransCipher, *errno.Errno) {
	return nil, nil
	// if reader == nil {
	// 	return nil, errno.TransCipherMaterialNilError
	// }

	// // 读取加密材料，长度固定为344字节：RSA-256加密后密文长度为256byte，base64编码后长度固定为344字节。
	// cipherMaterialBytes := make([]byte, 256)
	// _, err := io.ReadFull(reader, cipherMaterialBytes)
	// if err != nil {
	// 	return nil, errno.TransCipherMaterialUnexpectedEOfError.Append(err.Error())
	// }

	// // 加密材料protobuf反序列化
	// material := CipherMaterial{}
	// err = proto.Unmarshal(cipherMaterialBytes, &material)
	// if err != nil {
	// 	return nil, errno.TransCipherMaterialUnmarshalError.Append(err.Error())
	// }

	// DEK, err := base64.StdEncoding.DecodeString(material.GetPublickKeyCipherDEK())
	// if err != nil {
	// 	return nil, errno.TransCipherMaterialUnmarshalError
	// }

	// // 加密材料RSA解密
	// plainDek, err := s.rsaDecrypt(DEK)
	// if err != nil {
	// 	return nil, errno.TransCipherMaterialRSADecryptError.Append(err.Error())
	// }
	// IV, err := base64.StdEncoding.DecodeString(material.GetIV())
	// if err != nil {
	// 	return nil, errno.TransCipherMaterialUnmarshalError
	// }

	// return engine.NewEngineTransCipherStream(plainDek, IV, material.GetHmac(), cipherMode, reader)
}

// NewEngineFileCipherStream 创建流式文件加密引擎。
//  @param cipherKey 文件加密密钥的密文。如果加密文件时需要重新分配一个加密密钥，则应当传nil。如果需要使用同一个密钥，则传入第一次创建的密钥的密文。
//  @param blockSize 待加密或解密数据的分组长度，必须为16的整数倍。
//  @param reader 数据读取流，如文件流，网络流。
//  @return *engine.EngineFileCipher 流式文件加密引擎对象。
//  @return []byte 由KMS生成的加密密钥的密文，用户应当存储并妥善保存，丢失将无法解密文件。
//  @return *errno.Errno 如果失败，返回错误原因以及错误码。
// func (s *ServiceBasedTEE) NewEngineFileCipherStream(cipherKey []byte, blockSize uint64, cipherMode engine.CipherMode, reader io.Reader) (*engine.EngineFileCipherStream, []byte, *errno.Errno) {

// 	// material 私钥解密后的明文，里面有DEK明文
// 	material := CipherMaterial{}

// 	// cipherMaterial 被公钥加密的密文。
// 	cipherMaterial := make([]byte, 256)

// 	return engine.NewEngineFileCipherStream(material.GetKey(), cipherKey, blockSize, cipherMode, reader)
// }

// rsaDecrypt RSA解密加密材料。
//
//	@receiver s
//	@param ciphertext 加密材料密文。
//	@return []byte 加密材料明文。
//	@return error 如果出错，返回错误码以及详细信息。
// func (s *ServiceBasedTEE) rsaDecrypt(ciphertext []byte) ([]byte, error) {
// 	block, _ := pem.Decode(PrivateKey)
// 	if block == nil {
// 		return nil, errors.New("private key decode error!")
// 	}

// 	parseResult, err := x509.ParsePKCS8PrivateKey(block.Bytes)
// 	if err != nil {
// 		return nil, err
// 	}

// 	plaintext, err := rsa.DecryptOAEP(sha256.New(), rand.Reader, parseResult.(*rsa.PrivateKey), ciphertext, nil)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return plaintext, nil
// }
