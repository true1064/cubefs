package kms

import "github.com/pkg/errors"

const (
	ErrCodeDependencyTimeoutException          = "DependencyTimeoutException"
	ErrCodeInternalFailureException            = "InternalFailure"
	ErrCodeInvalidClientTokenIdException       = "InvalidClientTokenId"
	ErrCodeMissingAuthenticationTokenException = "MissingAuthenticationToken"
	ErrCodeInvalidActionException              = "InvalidActionException"
	ErrCodeInvalidParameterValueException      = "InvalidParameterValue"
	ErrCodeMissingActionException              = "MissingAction"
	ErrCodeNotAuthorizedException              = "NotAuthorized"
	ErrCodeRequestExpiredException             = "RequestExpired"
	ErrCodeThrottlingException                 = "ThrottlingException"
	ErrCodeAccessDeniedException               = "AccessDeniedException"
	ErrCodeInvalidArnException                 = "InvalidArnException"
	ErrCodeKMSInvalidStateException            = "KMSInvalidStateException"
	ErrCodeNotFoundException                   = "NotFoundException"
	ErrCodeDisabledException                   = "DisabledException"
	ErrCodeIncorrectKeyException               = "IncorrectKeyException"
	ErrCodeInvalidCiphertextException          = "InvalidCiphertextException"
	ErrCodeInvalidGrantTokenException          = "InvalidGrantTokenException"
	ErrCodeInvalidKeyUsageException            = "InvalidKeyUsageException"
	ErrCodeIncorrectKeyMaterialException       = "IncorrectKeyMaterialException"
	ErrCodeRepeatedAliasException              = "RepeatedAliasException"
	ErrCodeUnknowedErrorException              = "UnknowedError"
)

var (
	ErrRequestInit = errors.New("request uninitialized")
	// ErrJsonUnmarshal
	ErrJsonUnmarshal = errors.New("json unmarshal error")
	// ErrJsonMarshal
	ErrJsonMarshal = errors.New("json Marshal error")
	// ErrRequestSend
	ErrRequestSend = errors.New("request send failed")

	ErrCryptoIllegalKeyLength = errors.New("illegal key length")
	ErrCryptoGenerateRandom   = errors.New("generate random failed")
	ErrCryptoAESGCMEncrypt    = errors.New("AES-GCM encrypt failed")
	ErrCryptoAESGCMDecrypt    = errors.New("AES-GCM decrypt failed")
	ErrCryptoMarshalCMS       = errors.New("marshal cms failed")
	ErrCryptoUnmarshalCMS     = errors.New("unmarshal cms failed")
	ErrCryptoParamsInvalid    = errors.New("params invalid")
)
