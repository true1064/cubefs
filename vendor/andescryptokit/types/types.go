/*
Copyright OPPO Corp. All Rights Reserved.
*/

// package andescryptokit is used to define all interfaces exposed to users, and different
// encryption schemes should implement these interfaces.
package types

// EnvironmentType For describing the usage environment.
type EnvironmentType uint16

const (

	// EnvironmentDevelop test environment.
	EnvironmentDevelop EnvironmentType = 1

	// EnvironmentTest development environment.
	EnvironmentTest EnvironmentType = 2

	// EnvironmentProduct production environment.
	EnvironmentProduct EnvironmentType = 3
)

type CipherScheme uint16

const (

	// CipherScheme_Common 通用型密码套件：仅用作加解密、哈希运算等操作
	CipherScheme_Common CipherScheme = 1

	// CipherScheme_Service 服务级KMS加密：端、云均有能力解密用户数据
	CipherScheme_ServiceBasedKMS CipherScheme = 2

	// CipherScheme_Service 服务级TEE加密：端、云均有能力解密用户数据
	CipherScheme_ServiceBasedTEE CipherScheme = 3

	// CipherScheme_User 用户级加密：仅用户相同账号的设备能解密数据
	CipherScheme_User CipherScheme = 4

	// CipherScheme_Device 设备级加密：仅用户某台设备能够解密数据
	CipherScheme_Device CipherScheme = 5
)

// AuthParam AUTH配置参数
type AuthParam struct {

	// AppName 业务方应用名称，应当为PSA的A的名称。
	AppName string

	// AK 安全密钥控制台创建的Acess Key。
	AK string

	// SK 安全密钥控制台创建的Secret Key。
	SK string
}

// CustomMasterKey 云端KMS-NXG配置参数
type CustomMasterKey struct {
	// TransKeyId 安全密钥控制台创建的CMK ID，类型为非对称密钥，用于EngineTransCipherStream密钥派生。
	TransKeyId string

	// FileKeyId 安全密钥控制台创建的CMK ID，类型为对称密钥，用于EngineFileCipherStream密钥派生。
	// 某些加密方案（如ServiceBasedTEE）的EngineFileCipherStream密钥与终端TEE/SE绑定，因此不需要此参数，传入空字符串即可。
	FileKeyId string
}

// Configure KmsParam KMS鉴权参数，这些参数在云平台安全密钥控制台能够查询到。
type Configure struct {

	// env 使用环境：生产、开发、测试环境。
	Environment EnvironmentType

	// authParam AUTH配置参数
	AuthParam AuthParam

	// customMasterKey 云端KMS-NXG配置参数
	CustomMasterKey CustomMasterKey
}

// CipherMode 传输模式
type CipherMode uint16

const (

	// ENCRYPT_MODE 加密模式
	ENCRYPT_MODE CipherMode = 1

	// DECRYPT_MODE 解密模式
	DECRYPT_MODE CipherMode = 2
)

const (
	AuthUrl = "https://datasec-auth-cn.heytapmobi.com"

	// SERVICE_BASED_KMS_CIPHER_MATERIAL_LEN 加密材料长度
	SERVICE_BASED_KMS_TRNAS_CIPHER_MATERIAL_LEN = 341

	SERVICE_BASED_KMS_FILE_CIPHER_MATERIAL_LEN = 251
)

// KmsExUrl
var KmsExUrl = map[EnvironmentType]string{
	EnvironmentDevelop: "http://kmsex-dev.wanyol.com",
	EnvironmentTest:    "http://kmsex-test.wanyol.com",
	EnvironmentProduct: "http://kmsex-prod.wanyol.com",
}

// KmsNxgUrl
var KmsNxgUrl = map[EnvironmentType]string{
	EnvironmentDevelop: "http://kms-dev.wanyol.com",
	EnvironmentTest:    "http://kms-test.wanyol.com",
	EnvironmentProduct: "http://datasec-kms-cn.oppo.local",
}
