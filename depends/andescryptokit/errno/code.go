/*
Copyright OPPO Corp. All Rights Reserved.
*/

package errno

var (

	// OK 正确运行。
	OK = &Errno{code: 0, message: "OK"}

	/* InternalError 系统错误, 前缀为 100 */
	// InternalError 不存在的加密方案。
	InternalError = &Errno{code: 10001, message: "cipher scheme type error."}

	// UnsupportedCipherSchemeError 暂未实现的加密方案。
	UnsupportedCipherSchemeError = &Errno{code: 10002, message: "unsupported cipher scheme type error."}

	/* KMS错误，前缀为 200 */
	// KmsParamError KMS3.0参数错误。
	KmsParamError = &Errno{code: 20001, message: "KMS3.0 param error."}

	// KmsInitError KMS3.0初始化出错。
	KmsInitError = &Errno{code: 20002, message: "KMS3.0 init error."}

	// KmsNxgInitError KMS NXG初始化出错。
	KmsNxgInitError = &Errno{code: 20003, message: "KMS NXG init error."}

	/* 传输加密错误，前缀为 300 */
	// TransCipherKeyBase64DecodeError 传输加密密钥base64解码出错。
	TransCipherKeyBase64DecodeError = &Errno{code: 30001, message: "tran cipher param key base64 decode error."}

	// TransCipherKeyRsaDecryptError 传输加密密钥rsa解密出错。
	TransCipherKeyRsaDecryptError = &Errno{code: 30002, message: "tran cipher param key rsa decrypt error."}

	// TransCipherIVBase64DecodeError 传输加密初始化向量base64解码出错。
	TransCipherIVBase64DecodeError = &Errno{code: 30003, message: "tran cipher param IV base64 decode error."}

	// TransCipherKeyLengthError 传输加密密钥长度不正确，它的长度必须为32字节。
	TransCipherKeyLengthError = &Errno{code: 30004, message: "tran cipher param key length(must 32 bytes) error."}

	// TransCipherIVLengthError 传输加密初始化向量长度不正确，它的长度必须为16字节。
	TransCipherIVLengthError = &Errno{code: 30005, message: "tran cipher param IV length(must 16 bytes) error."}

	// TransCipherInternalError 传输加密引擎创建aes-256-ctr加密引擎出错。
	TransCipherInternalError = &Errno{code: 30006, message: "tran cipher create aes-256-ctr cipher error."}

	// TransCipherHmacWriteError 传输加密引擎计算HMAC时实际计算的长度与传入的数据长度不一致。
	TransCipherHmacWriteError = &Errno{code: 30007, message: "tran cipher hamc data did not fully written error."}

	// TransCipherHmacWriteClosedWithError 传输加密引擎计算HMAC时缓冲区被提前关闭。
	TransCipherHmacWriteClosedWithError = &Errno{code: 30008, message: "tran cipher hamc data closed with an error."}

	// TransCipherHmacOffError 传输加密引擎未开启HMAC计算，却被调用获取HMAC值。
	TransCipherHmacOffError = &Errno{code: 30009, message: "tran cipher hamc status off error."}

	// TransCipherCiphertextLenError 传输加密引擎加密时，密文长度小于明文长度。
	TransCipherCiphertextLenError = &Errno{code: 30010, message: "tran cipher ciphertext is smaller than plaintext error."}

	// TransCipherPlaintextLenError 传输加密引擎解密时，明文长度小于密文长度。
	TransCipherPlaintextLenError = &Errno{code: 30011, message: "tran cipher plaintext is smaller than ciphertext error."}

	// TransCipherMaterialNil 传输加密材料空指针错误。
	TransCipherMaterialNilError = &Errno{code: 30012, message: "tran cipher material is nil error."}

	// TransCipherMaterialUnmarshalError 传输加密材料protobuf反序列化失败错误。
	TransCipherMaterialBase64DecodeError = &Errno{code: 30013, message: "tran cipher material base64 decode error."}

	// TransCipherMaterialRSADecryptError 传输加密材料RSA解密失败。
	TransCipherMaterialRSADecryptError = &Errno{code: 30014, message: "tran cipher material rsa decrypt error."}

	// TransCipherMaterialUnmarshalError 传输加密材料protobuf反序列化失败错误。
	TransCipherMaterialUnmarshalError = &Errno{code: 30015, message: "tran cipher material protobuf unmarshal failed error."}

	// TransCipherMaterialUnexpectedEOfError 传输加密材料长度不够344字节错误。
	TransCipherMaterialUnexpectedEOfError = &Errno{code: 30016, message: "tran cipher material len error: unexpected EOF."}

	// TransCipherStreamModeNilStreamError 传输加密传入空流错误。
	TransCipherStreamModeNilStreamError = &Errno{code: 40010, message: "trans cipher stream mode nil stream error."}

	// TransCipherStreamModeSetDataReaderError 传输加密设置新数据流为空错误
	TransCipherStreamModeSetNilDataReaderError = &Errno{code: 40011, message: "trans cipher stream mode set nil data reader error."}

	// TransCipherStreamModeSetDataReaderError 传输加密设置新数据流错误
	TransCipherStreamModeSetDataReaderError = &Errno{code: 40011, message: "trans cipher stream mode set data reader error."}

	/*  文件存储错误，前缀为 400 */
	// FileCipherKeyLengthError 文件加密密钥长度错误，必须为32字节或者64字节。
	FileCipherKeyLengthError = &Errno{code: 40001, message: "file cipher param key length(must 32 or 64 bytes) error."}

	// FileCipherBlockSizeError 文件加密分组长度错误，必须为16的整数倍。
	FileCipherBlockSizeError = &Errno{code: 40002, message: "file cipher block size(must be a multiple of 16 bytes) error."}

	// FileCipherNewXtsCipherError 文件加密引擎创建aes-xts对象错误。
	FileCipherNewXtsCipherError = &Errno{code: 40003, message: "file cipher new aes-xts error."}

	// FileCipherBlockModePlaintextLengthError 随机加密数据长度与设置的分组长度不一致错误。
	FileCipherBlockModePlaintextLengthError = &Errno{code: 40004, message: "file cipher block mode plaintext length is little than ciphertext error."}

	// FileCipherFileModePlaintextLengthError 文件加密明文长度大于设置的分组长度错误。
	FileCipherFileModePlaintextLengthError = &Errno{code: 40005, message: "file cipher file mode plaintext length cannot be greater than block size error."}

	// FileCipherBlockModeCiphertextLengthError 随机解密数据长度与设置的分组长度不一致错误。
	FileCipherBlockModeCiphertextLengthError = &Errno{code: 40006, message: "file cipher block mode ciphertext length must be equal to block size error."}

	// FileCipherFileModeCipherextLengthError 文件加密密文长度大于设置的分组长度错误。
	FileCipherFileModeCipherextLengthError = &Errno{code: 40007, message: "file cipher file mode ciphertext length cannot be greater than block size error."}

	// FileCipherFileModeTotalBlockLessThanOneError 文件加密明文或者密文的总长度不足一个分组大小的错误。
	FileCipherFileModeTotalBlockLessThanOneError = &Errno{code: 40008, message: "file cipher file mode total block number is less than one error."}

	// FileCipherBlockModeSectorNumError 文件加密分组序号错误。
	FileCipherBlockModeSectorNumError = &Errno{code: 40009, message: "file cipher block mode sector number error."}

	// FileCipherBlockModeSectorNumError 文件加密传入空流错误。
	FileCipherStreamModeNilStreamError = &Errno{code: 40010, message: "file cipher stream mode nil stream error."}

	/*  文件存储错误，前缀为 500 */
	ServiceBasedDataEncryptKeyError = &Errno{code: 50001, message: "service based scheme data encrypt key(DEK) get error."}

	/*  引擎AES-256-GCM错误，前缀为 600 */
	EngineAes256GcmKeySizeError = &Errno{code: 60001, message: "engine aes-256-gcm key length error."}
)
