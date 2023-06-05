package types

import authType "andescryptokit/osec/oppo_authentication_sdk_go/service/seckit_auth/types"

type CredentialType int32

// CredentialType 凭据类型
const (
	AKSK CredentialType = iota
	CERT
	USERNAME
	TICKET
)

type EncAlgType int32

// EncAlgType 加密算法
const (
	NO_ENC_ALG            EncAlgType = iota // 不加密
	AES_128_GCM_NOPADDING                   // AES-128/GCM/NoPadding
	AES_128_CTR_NOPADDING                   // AES-128/CTR/NoPadding
	AES_128_CBC_PKCS7                       // AES-128/CBC/PKCS7Padding
	AES_256_GCM_NOPADDING                   // AES-256/GCM/NoPadding
	AES_256_CTR_NOPADDING                   // AES-256/CTR/PKCS7Padding
	AES_256_CBC_PKCS7                       // AES-256/CBC/NoPadding
	SM4_GCM_NOPADDING                       // SM4/GCM/NoPadding
	SM4_CTR_NOPADDING                       // SM4/CTR/NoPadding
	SM4_CBC_PKCS7                           // SM4/CBC/PKCS7Padding
)

type SigAlgType int32

// SigAlgType
const (
	NO_SIG_ALG SigAlgType = iota // 不签名
	HS256                        // HMAC-SHA-256
	HS384                        // HMAC-SHA-384
	HS512                        // HMAC-SHA-512
	ECDSA                        // 根据证书或私钥⾃动推断
	RSA                          // 根据证书或私钥⾃动推断
	ECDSA_P256
	ECDSA_P384
	ECDSA_P521
	RSA_2048_PKCS1_v15
	RSA_3072_PKCS1_v15
	RSA_4096_PKCS1_v15
	RSA_2048_PKCS1_PSS
	RSA_3072_PKCS1_PSS
	RSA_4096_PKCS1_PSS
)

type PermissionLevel int

const (
	INVALID     PermissionLevel = iota // 无授权
	APPLICATION                        // 应用授权
	RESOURCE                           // 资源授权
)

type AuthClientReq struct {
	Header         string `json:"header"`
	Body           string `json:"body"`
	Signature      string `json:"signature"`
	AuthProtoGuard int64  `json:"auth_proto_guard"`
}

type GetAuthTicketReqBody struct {
	CName      string                  `json:"c_name"`
	SName      string                  `json:"s_name"`
	EntityInfo *authType.AppEntityInfo `json:"entity_info"`
}

type GetAuthTicketReqHeader struct {
	Version        int32          `json:"version"`
	RequestID      string         `json:"request_id"`
	CredentialId   string         `json:"credential_id"`
	CredentialType CredentialType `json:"credential_type"`
	EncAlg         EncAlgType     `json:"enc_alg"`   // 加密算法
	SigAlg         SigAlgType     `json:"sig_alg"`   // 签名算法
	Timestamp      uint64         `json:"timestamp"` // 时间戳，UTC时间，RFC3339格式
	Nonce          string         `json:"nonce"`
}

type GetAuthTicketReqParam struct {
	AuthClientReq
}

type GetAuthTicketRespBody struct {
	CName      string `json:"c_name"`
	SName      string `json:"s_name"`
	SessionKey string `json:"session_key"`
	Ticket     string `json:"ticket"`
}

type GetAuthTicketRespParam struct {
	AuthClientReq
}

type Pack struct {
	AuthClientReq
}

type Header struct {
	CName          string         `json:"c_name"`
	SName          string         `json:"s_name"`
	Version        int32          `json:"version"`
	RequestID      string         `json:"request_id"`
	CredentialId   string         `json:"credential_id"`
	CredentialType CredentialType `json:"credential_type"`
	EncAlg         EncAlgType     `json:"enc_alg"`   // 加密算法
	SigAlg         SigAlgType     `json:"sig_alg"`   // 签名算法
	Timestamp      uint64         `json:"timestamp"` // 时间戳，UTC时间，RFC3339格式
	Nonce          string         `json:"nonce"`
	AuthenTicator  string         `json:"authenticator"`
	Ticket         string         `json:"ticket"`
}

type GetPeerTicketReqParam struct {
	PeerName string `json:"peer_name"`
}

type GetPeerTicketRespParam struct {
	CName      string               `json:"c_name"`
	SName      string               `json:"s_name"`
	SessionKey *authType.SessionKey `json:"session_key"`
	Ticket     string               `json:"ticket"`
}

type GetSecPolicyRespParam struct {
	DegradeEnable   bool   `json:"degrade_enable"`
	SecLevel        int    `json:"sec_level"`
	KekRotatePeriod uint32 `json:"kek_rotate_period"`
	TicketLifetime  uint32 `json:"ticket_lifetime"`
}

type GetKeksRespParam struct {
	Keks           []*authType.Kek `json:"keks"`
	TicketLifetime uint64          `json:"ticket_lifetime"`
}

type GetPermissionsReqParam struct {
	AppNameList []string `json:"app_name_list"`
}

type AppPermission struct {
	AppName            string                     `json:"app_name"`
	PermissionLevel    PermissionLevel            `json:"permission_level"`
	PermissionItemList []*authType.PermissionItem `json:"permission_item_list"`
}

type GetPermissionsRespParam struct {
	AppPermissions []*AppPermission `json:"app_permissions"`
}

type GetPermittedClientsRespParam struct {
	PermittedClientList []*PermittesClient `json:"permitted_client_list"`
}

type PermittesClient struct {
	AppName         string `json:"app_name"`
	PermissionLevel int    `json:"permission_level"`
}

type GetDisabledClientsRespParam struct {
	DisabledClients []*DisableClient `json:"disabled_clients"`
}

type DisableClient struct {
	HostName  string `json:"host_name"`
	AppName   string `json:"app_name"`
	BeginTime uint64 `json:"begin_time"`
	EndTIme   uint64 `json:"end_time"`
}

type VerifyTokenReqParam struct {
	Token string `json:"token"`
}

type VerifyTokenRespParam struct {
	UserStatus        int                `json:"userStatus"`
	UserArn           string             `json:"user_arn"`
	UserId            string             `json:"user_id"`
	AccountId         string             `json:"account_id"`
	BasicRole         int32              `json:"basic_role"`
	Policy            string             `json:"policy"`
	FederalIdentities []*FederalIdentity `json:"federal_identities"`
}

type FederalIdentity struct {
	IDP      string `json:"idp"`
	UserId   string `json:"user_id"`
	UserName string `json:"user_name"`
}

type GetUserCredentialReqParam struct {
	AccessKeyId string `json:"access_key_id"`
}

type GetUserCredentialRespParam struct {
	UserArn         string `json:"user_arn"`
	UserId          string `json:"user_id"`
	AccountId       string `json:"account_id"`
	AccessKeyId     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	AKExp           int64  `json:"access_key_exp"`
	BasicRole       int32  `json:"basic_role"`
	AccessKeyStatus int    `json:"access_key_status"`
}

type GetUsersInfoReqParam struct {
	UserArns []string `json:"user_arns"`
}

type GetUsersInfoRespParam struct {
	Users []*User `json:"users"`
}

type User struct {
	UserARN         string            `json:"user_arn"`
	UserID          string            `json:"user_id"`
	UserName        string            `json:"user_name"`
	AccountID       string            `json:"account_id"`
	BasicRole       int32             `json:"basic_role"`
	Policy          string            `json:"policy"`
	FederalIdentity []FederalIdentity `json:"federal_identities"`
	AccessKeys      []AccessKey       `json:"access_keys"`
}

type AccessKey struct {
	AK              string `json:"access_key_id"`
	SK              string `json:"secret_access_key"`
	Exp             int64  `json:"access_key_exp"`
	AccessKeyStatus int    `json:"access_key_status"`
}

type GetAllUserBriefRespParam struct {
	UserARN []string `json:"user_arn"`
}
