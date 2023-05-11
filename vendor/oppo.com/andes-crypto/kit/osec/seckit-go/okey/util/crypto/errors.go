package okeycrypto

import (
	"errors"
)

var (
	ErrCryptoIllegalKeyLength                = errors.New("illegal key length")
	ErrCryptoGenerateRandom                  = errors.New("generate random failed")
	ErrCryptoAESGCMEncrypt                   = errors.New("AES-256-GCM encrypt failed")
	ErrCryptoAESGCMDecrypt                   = errors.New("AES-256-GCM decrypt failed")
	ErrCryptoPBMarshalCMS                    = errors.New("PB marshal cms failed")
	ErrCryptoPBUmarshalCMS                   = errors.New("PB umarshal cms failed")
	ErrCryptoCMSPaddingNotSupported          = errors.New("cms padding not supported")
	ErrCryptoCMSEncAlgNotSupported           = errors.New("cms eng alg not supported")
	ErrCryptoCMSHashIDNotSupported           = errors.New("cms hash id not supported")
	ErrCryptoCMSSignAlgNotSupported          = errors.New("cms sign alg not supported")
	ErrCryptoCMSVerify                       = errors.New("cms verify failed")
	ErrCryptoParamsInvalid                   = errors.New("params invalid")
	ErrCryptoEnvelopDecrptFailed             = errors.New("envelope decrypt failed")
	ErrCryptoEnvelopDecrptFailedCipherFormat = errors.New("envelope cipher format invalid")
	ErrCryptoEnvelopDecrptFailedsCertInvalid = errors.New("envelope certificate invalid")
	ErrCryptoNativeError                     = errors.New("envelope native decrypt failed")
)
