/*
Copyright OPPO Corp. All Rights Reserved.
*/

// package server is used to define all interfaces exposed to users, and different
// encryption schemes should implement these interfaces.
package server

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

	// CipherScheme_Service 服务级加密：端、云均有能力解密用户数据
	CipherScheme_Service CipherScheme = 2

	// CipherScheme_User 用户级加密：仅用户相同账号的设备能解密数据
	CipherScheme_User CipherScheme = 3

	// CipherScheme_Device 设备级加密：仅用户某台设备能够解密数据
	CipherScheme_Device CipherScheme = 4
)

// KmsParam KMS鉴权参数，这些参数在云平台安全密钥控制台能够查询到。
type KmsParam struct {

	// AppName 业务方应用名称，应当为PSA的A的名称。
	AppName string

	// AK 安全密钥控制台创建的Acess Key。
	AK string

	// SK 安全密钥控制台创建的Secret Key。
	SK string

	// KeyId 安全密钥控制台创建的CMK ID.
	KeyId string
}
