package okey

var (
	GET_KMS_TICKET        = "getkmsticket"
	GET_KEK               = "getkek"
	GET_KMS_SYSTEM_TIME   = "getkmssystemtime"
	DOWNLOAD_WB_KEY       = "downloadwbkey"
	DOWNLOAD_WB_KEYS      = "downloadwbkeys"
	DOWNLOAD_WB           = "downloadwb"
	REPORT_WB_USAGE_STATS = "reportwbusagestats"
	GET_SECURITY_POLICY   = "getsecuritypolicy"
	GET_GRANTED_APP_INFO  = "getgrantedappinfo"
	GET_GRANTED_APP_NAME  = "getgrantedappname"
	DOWNLOAD_KEY_PAIR     = "downloadKeyPair"
)

var (
	KMS_URL_DEV  = "http://kmsex-dev.wanyol.com"
	KMS_URL_TEST = "http://kmsex-test.wanyol.com"
	KMS_URL_PROD = "http://datasec-kmsex-cn.oppo.local"

	AUTH_URL_DEV  = "https://datasec-auth-cn.heytapmobi.com"
	AUTH_URL_TEST = "https://datasec-auth-cn.heytapmobi.com"
	AUTH_URL_PROD = "http://datasec-iam-cn.oppo.local"
)

var (
	DEV  = 1
	TEST = 2
	PROD = 3
)

var MAX_SERVER = 5000

var (
	KMS_URL_MAP  map[int]string = make(map[int]string)
	AUTH_URL_MAP map[int]string = make(map[int]string)
)

var (
	KEY_TYPE_WB          = "WB"
	KEY_TYPE_SESSION_KEY = "SessionKey"
)

// define algorithm parameter name
var (
	AES_256_GCM = "AES-256-GCM"
	AES_192_GCM = "AES-192-GCM"
	AES_128_GCM = "AES-128-GCM"

	AES_256_CTR = "AES-256-CTR"
	AES_192_CTR = "AES-192-CTR"
	AES_128_CTR = "AES-128-CTR"

	AES_256_OFB = "AES-256-OFB"
	AES_192_OFB = "AES-192-OFB"
	AES_128_OFB = "AES-128-OFB"

	AES_256_CFB = "AES-256-CFB"
	AES_192_CFB = "AES-192-CFB"
	AES_128_CFB = "AES-128-CFB"

	AES_256_ECB = "AES-256-ECB"
	AES_192_ECB = "AES-192-ECB"
	AES_128_ECB = "AES-128-ECB"

	AES_256_CBC                = "AES-256-CBC"
	AES_192_CBC                = "AES-192-CBC"
	AES_128_CBC                = "AES-128-CBC"
	RSA_NONE_PKCS1Padding      = "RSA/None/PKCS1Padding"
	RSA_SHA256_PKCSOAEPPadding = "RSA/SHA256/PKCS1_OAEPPadding"
	RSA_SHA384_PKCSOAEPPadding = "RSA/SHA384/PKCS1_OAEPPadding"
	RSA_SHA512_PKCSOAEPPadding = "RSA/SHA512/PKCS1_OAEPPadding"

	NO_PADDING = "NoPadding"

	HMAC   = "HMAC"
	SHA256 = "SHA256"
)

var (
	KeyTypeWB         = "WB"
	KeyTypeSessionKey = "SessionKey"
)
