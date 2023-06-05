package okey

import (
	"encoding/base64"
	"encoding/json"

	kmserr "andescryptokit/osec/seckit-go/okey/error"
	"andescryptokit/osec/seckit-go/okey/model"
	"andescryptokit/osec/seckit-go/okey/util"
	okeycrypto "andescryptokit/osec/seckit-go/okey/util/crypto"

	cmap "github.com/orcaman/concurrent-map"
	"github.com/pkg/errors"
)

type KmsExCipher struct {
	ENVELOPE_VERSION string
	WORK_MODE        string

	DEK_ENC_CIPHER_SUITE string

	AppName            string
	EncryptAlgorithm   string
	SignatureAlgorithm string
	HashId             string
	Padding            string
	DoDecrypt          bool

	Pack           *model.Pack
	SignedData     *model.CMSSignedData
	Header         *model.Header
	Payload        *model.CMSEncryptedData
	SecurityPolicy *model.SecurityPolicy
	Envelope       *model.Envelope

	KmsTransObj        *KmsTransmission
	Kek                []byte
	SignaturePlain     []byte
	WBMap              cmap.ConcurrentMap
	KeyPairMap         cmap.ConcurrentMap
	UNAUTHENTICATEDint int
	TicketType         int
	LastAccessTime     int64

	Mk  []byte
	Dek []byte

	WbId  string
	KeyId string
}

type CipherParams struct {
	AppName            string
	KmsTransObj        *KmsTransmission
	Envelope           *model.Envelope
	KeyPairMap         cmap.ConcurrentMap
	Kek                []byte
	SignedData         *model.CMSSignedData
	Header             *model.Header
	Payload            *model.CMSEncryptedData
	SecurityPolicy     *model.SecurityPolicy
	WBMap              cmap.ConcurrentMap
	EncryptAlgorithm   string
	Padding            string
	SignatureAlgorithm string
	HashId             string
	SignaturePlain     []byte
}

// GetKmsExCipher ...
func GetKmsExCipher(param CipherParams) (*KmsExCipher, error) {
	kmsExCipher := &KmsExCipher{}
	kmsExCipher.AppName = param.AppName
	kmsExCipher.KmsTransObj = param.KmsTransObj
	kmsExCipher.Envelope = param.Envelope
	kmsExCipher.KeyPairMap = param.KeyPairMap
	kmsExCipher.Kek = param.Kek
	kmsExCipher.SignedData = param.SignedData
	kmsExCipher.Header = param.Header
	kmsExCipher.Payload = param.Payload
	kmsExCipher.SecurityPolicy = param.SecurityPolicy
	kmsExCipher.WBMap = param.WBMap
	kmsExCipher.EncryptAlgorithm = param.EncryptAlgorithm
	kmsExCipher.Padding = param.Padding
	kmsExCipher.SignatureAlgorithm = param.SignatureAlgorithm
	kmsExCipher.HashId = param.HashId
	kmsExCipher.SignaturePlain = param.SignaturePlain

	return kmsExCipher, nil
}

// Init ... KmsExCipher Init
func (ec *KmsExCipher) Init() error {
	// get security policy
	secPolicy, err := ec.KmsTransObj.GetSecurityPolicy(ec.AppName)
	if err != nil {
		return err
	}
	tmpPolicy := &model.SecurityPolicy{
		DegradeFlag:       secPolicy.AllowDegradation,
		TicketDegradeFlag: secPolicy.AllowUnauthenticated,
		TicketExpired:     false,
	}
	ec.SecurityPolicy = tmpPolicy

	ec.WBMap = cmap.New()
	ec.KeyPairMap = cmap.New()
	// get server appname's wb keys
	err = ec.UpdateWhiteBoxKeys()
	if err != nil {
		return err
	}

	return nil
}

// UpdateWhiteBoxKeys ...
func (ec *KmsExCipher) UpdateWhiteBoxKeys() error {
	respWBKeys, err := ec.KmsTransObj.DownloadWbKeys(ec.AppName, uint64(ec.LastAccessTime))
	if err != nil {
		return err
	}
	wbs := respWBKeys.Wbs

	// put wb infos into wbmap
	for _, wbInfo := range wbs {
		wbId := wbInfo.WbId
		wbKeys := wbInfo.WbKey
		// handle wbKeys

		wbKeyMap := cmap.New()
		for _, wbKeyInfo := range wbKeys {
			keyBytes, _ := base64.StdEncoding.DecodeString(wbKeyInfo.Key)
			wbKeyMap.Set(wbKeyInfo.KeyId, keyBytes)
			// ec.WBMap.Set(wbId, wbKeyMap)
		}
		ec.WBMap.Set(wbId, wbKeyMap)
	}

	return nil
}

// GenMkAndDek ...
func (ex *KmsExCipher) GenMkAndDek() error {
	if ex.Header.KeyType == KEY_TYPE_WB {
		wbId := ex.Header.WbKeyIndex.WbId
		wbKeyId := ex.Header.WbKeyIndex.KeyId
		wbKeyMap, isOk := ex.WBMap.Get(wbId)
		if !isOk {
			return errors.WithMessagef(kmserr.ErrPackDataInvalid, "wb not found, appname(%v), wbId(%v), keyId(%v)", ex.AppName, wbId, wbKeyId)
		}
		if wbKeyMap == nil {
			return errors.WithMessagef(kmserr.ErrPackDataInvalid, "wb not found, appname(%v), wbId(%v), keyId(%v)", ex.AppName, wbId, wbKeyId)
		}

		// handle wbkey map
		wbKey, isOk := wbKeyMap.(cmap.ConcurrentMap).Get(wbKeyId)
		if !isOk {
			return errors.WithMessagef(kmserr.ErrPackDataInvalid, "wbkey not found, appname(%v), wbId(%v), keyId(%v)", ex.AppName, wbId, wbKeyId)
		}
		if wbKey == nil {
			return errors.WithMessagef(kmserr.ErrPackDataInvalid, "wbkey not found, appname(%v), wbId(%v), keyId(%v)", ex.AppName, wbId, wbKeyId)
		}

		ex.Mk = wbKey.([]byte)
		ex.Dek = wbKey.([]byte)
		return nil
	}

	// parse ticket
	if ex.Header.Ticket == "" {
		return errors.WithMessagef(kmserr.ErrPackDataInvalid, "ticket is null, appname: %v", ex.AppName)
	}
	ticketJson := &model.Ticket{}
	// decrypt ticket cipher
	ticketBytes, err := okeycrypto.CMSDecryptJson([]byte(ex.Header.Ticket), ex.Kek)
	if err != nil {
		return errors.WithMessagef(kmserr.ErrPackDataInvalid, "ticket CMSDecryptJson failed, appname(%v)", ex.AppName)
	}

	err = json.Unmarshal(ticketBytes, ticketJson)
	if err != nil {
		return errors.WithMessagef(kmserr.ErrPackDataInvalid, "ticket json format, appname(%v)", ex.AppName)
	}

	err = util.CheckExpired(ticketJson.BeginTime, ticketJson.EndTime)
	if err != nil {
		ex.SecurityPolicy.TicketExpired = true
	}

	ex.TicketType = int(ticketJson.TicketType)
	ex.Mk, _ = base64.StdEncoding.DecodeString(ticketJson.Mk)
	ex.Dek, _ = base64.StdEncoding.DecodeString(ticketJson.Dek)

	return nil
}

// EnvelopeDecrypt ...
func (ex *KmsExCipher) EnvelopeDecrypt() ([]byte, error) {
	var plainText []byte
	pk := ex.Envelope.Pk
	pkType := pk.PkType
	if pkType != "rsa" {
		return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "pktype(%v) invalid", pkType)
	}
	pkBitLength := pk.PkBitLength
	if pkBitLength != 2048 && pkBitLength != 3072 && pkBitLength != 4096 {
		return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "pk bit length(%v) invalid", pkBitLength)
	}
	pkHashValue := pk.PkHashValue
	if pkHashValue == "" {
		return nil, errors.WithMessage(kmserr.ErrPackDataInvalid, "pk hashvalue nil")
	}

	// get private sk of rsa
	tmpKeyPair, isOk := ex.KeyPairMap.Get(pkHashValue)
	if !isOk {
		return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "Key not found,Invalid pk_hash_value:%v,  app name:%v", pkHashValue, ex.AppName)
	}
	keyPair := tmpKeyPair.(model.KeyPair)
	sk := keyPair.SK

	dek := ex.Envelope.Dek
	dekEncCipherSuite := dek.DekEncCipherSuite
	if dekEncCipherSuite != "RSA/None/PKCS1Padding" &&
		dekEncCipherSuite != "RSA/SHA256/PKCS1_OAEPPadding" &&
		dekEncCipherSuite != "RSA/SHA384/PKCS1_OAEPPadding" &&
		dekEncCipherSuite != "RSA/SHA512/PKCS1_OAEPPadding" {
		return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "not support dek_enc_cipher_suite(%v)", dekEncCipherSuite)
	}

	transform := dekEncCipherSuite
	dekBitLength := dek.DekBitLength
	if dekBitLength != 128 && dekBitLength != 256 {
		return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "not support dek bit length(%v)", dekBitLength)
	}

	encryptedDek := dek.EncryptedDek
	if encryptedDek == "" {
		return nil, errors.WithMessage(kmserr.ErrPackDataInvalid, "encrypted_dek nil")
	}

	// decrypt dek cipher
	dekCipher, err := base64.StdEncoding.DecodeString(encryptedDek)
	if err != nil {
		return nil, errors.WithMessage(kmserr.ErrPackDataInvalid, "invalid encrypted_dek ")
	}

	ek, err := okeycrypto.RSADecrypt(transform, []byte(sk), dekCipher)
	if err != nil {
		return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "RSADecrypt failed, %v", err)
	}

	// verify envelope signature
	payload := ex.Envelope.Payload
	encryptedPayloadHmac := payload.EncryptedPayloadHmac
	if encryptedPayloadHmac == "" {
		return nil, errors.WithMessage(kmserr.ErrPackDataInvalid, "encryptedPayloadHmac nil")
	}
	signature, err := base64.StdEncoding.DecodeString(encryptedPayloadHmac)
	if err != nil {
		return nil, errors.WithMessage(kmserr.ErrPackDataInvalid, "invalid encryptedPayloadHmac ")
	}
	encryptedPayload := payload.EncryptedPayload
	if encryptedPayload == "" {
		return nil, errors.WithMessage(kmserr.ErrPackDataInvalid, "encryptedPayload nil")
	}
	payloadCipher, err := base64.StdEncoding.DecodeString(encryptedPayload)
	if err != nil {
		return nil, errors.WithMessage(kmserr.ErrPackDataInvalid, "invalid encryptedPayload ")
	}

	verifed, err := okeycrypto.VerifyWithHmacSHA256(payloadCipher, signature, ek)
	if err != nil {
		return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "do VerifyWithHmacSHA256() failed, %v", err)
	}
	if !verifed {
		return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "verify failed, %v", err)
	}

	// decrypt ciphertext
	iv, err := base64.StdEncoding.DecodeString(payload.PayloadIv)
	if err != nil {
		return nil, errors.WithMessage(kmserr.ErrPackDataInvalid, "invalid PayloadIv ")
	}
	if len(iv) != 12 && len(iv) != 16 {
		return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "invalid iv length(%v)", len(iv))
	}
	tag, err := base64.StdEncoding.DecodeString(payload.PayloadTag)
	if err != nil {
		return nil, errors.WithMessage(kmserr.ErrPackDataInvalid, "invalid PayloadTag ")
	}
	aad, err := base64.StdEncoding.DecodeString(payload.PayloadAad)
	if err != nil {
		return nil, errors.WithMessage(kmserr.ErrPackDataInvalid, "invalid PayloadAad ")
	}

	if payload.PayloadCipherSuite != "AES/GCM/NoPadding" {
		return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "invalid transform(%v)", payload.PayloadCipherSuite)
	}

	cipherText := append(payloadCipher, tag...)
	plainText, err = okeycrypto.AESGCMDecrypt(ek, cipherText, iv, aad)
	if err != nil {
		return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "AESGCMDecrypt failed, %v", err)
	}

	return plainText, nil
}

// Decrypt ...
func (ex *KmsExCipher) Decrypt() ([]byte, error) {
	if ex.Envelope != nil {
		plainText, err := ex.EnvelopeDecrypt()
		if err != nil {
			return nil, err
		}
		return plainText, nil
	}

	if ex.Payload == nil {
		return nil, errors.WithMessage(kmserr.ErrPackDataInvalid, "payload is nil")
	}

	keyType := ex.Header.KeyType
	if keyType == KEY_TYPE_SESSION_KEY {
		// verify signature
		err := okeycrypto.CMSVerifyJson([]byte(ex.Pack.Signature), ex.Mk, []byte(ex.Pack.Header), []byte(ex.Pack.Payload))
		if err != nil {
			return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "cms verify failed, err: %v", err)
		}
		// decrypt payload
		plain, err := okeycrypto.CMSDecryptJson([]byte(ex.Pack.Payload), ex.Dek)
		if err != nil {
			return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "cms decrypt failed, err: %v", err)
		}
		ex.DoDecrypt = true
		return plain, nil

	} else if keyType == KEY_TYPE_WB {
		// verify signature
		err := okeycrypto.CMSVerifyJson([]byte(ex.Pack.Signature), ex.Mk, []byte(ex.Pack.Header), []byte(ex.Pack.Payload))
		if err != nil {
			// fmt.Printf("mk: %v\n", ex.Mk)
			return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "cms verify failed, err: %v, appname(%v), wbid(%v), keyid(%v)", err, ex.AppName, ex.Header.WbKeyIndex.WbId, ex.Header.WbKeyIndex.KeyId)
		}
		// decrypt payload
		plain, err := okeycrypto.CMSDecryptJson([]byte(ex.Pack.Payload), ex.Dek)
		if err != nil {
			return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "cms decrypt failed, err: %v", err)
		}
		ex.DoDecrypt = true
		return plain, nil
	} else {
		return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "not support keytype(%v)", keyType)
	}
}

// Encrypt ...
func (ex *KmsExCipher) Encrypt(plain []byte) ([]byte, error) {
	if !ex.DoDecrypt {
		return nil, errors.WithMessage(kmserr.ErrPackDataInvalid, "must do decryption before encryption")
	}

	if plain == nil {
		return nil, kmserr.ErrParasInvalid
	}

	keyType := ex.Header.KeyType
	var cipherText []byte
	var signature []byte
	var err error

	if keyType == KEY_TYPE_SESSION_KEY {
		var aad []byte
		if ex.Payload.Aad == "" {
			aad = nil
		} else {
			aad = []byte(ex.Payload.Aad)
		}
		cipherText, err = okeycrypto.CMSEncryptJson(plain, ex.Dek, aad)
		if err != nil {
			return nil, errors.WithMessagef(kmserr.ErrInternal, "cms encrypt failed, %v", err)
		}
		signature, err = okeycrypto.CMSSignJson(ex.Mk, []byte(ex.Pack.Header), cipherText)
		if err != nil {
			return nil, errors.WithMessagef(kmserr.ErrInternal, "cms sign failed, %v", err)
		}
	} else if keyType == KEY_TYPE_WB {
		var aad []byte
		if ex.Payload.Aad == "" {
			aad = nil
		} else {
			aad = []byte(ex.Payload.Aad)
		}
		cipherText, err = okeycrypto.CMSEncryptJson(plain, ex.Dek, aad)
		if err != nil {
			return nil, errors.WithMessagef(kmserr.ErrInternal, "cms encrypt failed, err: %v, appname(%v), wbid(%v), keyid(%v)", err, ex.AppName, ex.Header.WbKeyIndex.WbId, ex.Header.WbKeyIndex.KeyId)
		}
		signature, err = okeycrypto.CMSSignJson(ex.Mk, []byte(ex.Pack.Header), cipherText)
		if err != nil {
			return nil, errors.WithMessagef(kmserr.ErrInternal, "cms sign failed, err: %v, appname(%v), wbid(%v), keyid(%v)", err, ex.AppName, ex.Header.WbKeyIndex.WbId, ex.Header.WbKeyIndex.KeyId)
		}
	} else {
		return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "keytype(%v) invalid", keyType)
	}

	pack := &model.Pack{
		Header:    ex.Pack.Header,
		Payload:   string(cipherText),
		Signature: string(signature),
	}

	packByte, _ := json.Marshal(pack)
	return packByte, nil
}

// MacVerify ...
func (ex *KmsExCipher) MacVerify(plain []byte) (bool, error) {
	if plain == nil {
		return false, kmserr.ErrParasInvalid
	}
	keyType := ex.Header.KeyType

	if keyType == KEY_TYPE_SESSION_KEY {
		err := okeycrypto.CMSVerifyJson([]byte(ex.Pack.Signature), ex.Mk, plain)
		if err != nil {
			ex.DoDecrypt = true
			return false, err
		}
		ex.DoDecrypt = true
	} else if keyType == KEY_TYPE_WB {
		err := okeycrypto.CMSVerifyJson([]byte(ex.Pack.Signature), ex.Mk, plain)
		if err != nil {
			ex.DoDecrypt = true
			return false, errors.WithMessagef(kmserr.ErrPackDataInvalid, "cms verify failed, appname(%v), wbId(%v)", ex.AppName, ex.Header.WbKeyIndex.WbId)
		}
		ex.DoDecrypt = true
	} else {
		return false, errors.WithMessagef(kmserr.ErrPackDataInvalid, "keytype(%v) invalid", keyType)
	}
	return true, nil
}

// MacSign ...
func (ex *KmsExCipher) MacSign(plain []byte) ([]byte, error) {
	if !ex.DoDecrypt {
		return nil, errors.WithMessage(kmserr.ErrPackDataInvalid, "must do decryption before encryption")
	}
	if plain == nil {
		return nil, kmserr.ErrParasInvalid
	}
	keyType := ex.Header.KeyType
	var cmsSignature []byte
	var err error

	if keyType == KEY_TYPE_SESSION_KEY {
		cmsSignature, err = okeycrypto.CMSSignJson(ex.Mk, plain)
		if err != nil {
			return nil, err
		}
	} else if keyType == KEY_TYPE_WB {
		cmsSignature, err = okeycrypto.CMSSignJson(ex.Mk, plain)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "keytype(%v) invalid", keyType)
	}

	payload := &model.Payload{}
	payloadByte, _ := json.Marshal(payload)
	pack := &model.Pack{
		Header:    ex.Pack.Header,
		Payload:   string(payloadByte),
		Signature: string(cmsSignature),
	}

	packByte, _ := json.Marshal(pack)
	return packByte, nil
}
