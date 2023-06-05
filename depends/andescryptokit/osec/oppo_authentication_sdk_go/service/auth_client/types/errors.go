package types

const (
	AUTH_ERR_BAD_SERVICE         = "service app not found"
	AUTH_ERR_NO_PERMISSION       = "no permission to service"
	AUTH_ERR_INVALID_CREDENTIAL  = "invalid credential"
	AUTH_ERR_EXPIRED_CREDENTIAL  = "credential expired"
	AUTH_ERR_HEALTH_CHECK_FAILED = "health check failed"
)

const (
	AuthReqErrInvalidCredential = 40300001
	AuthReqErrCredentialExpired = 40300002
	AuthReqErrNoPermission      = 40300006
	AuthReqErrBadService        = 40000007
)
