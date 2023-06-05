package kms

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

const (
	EnvCodeDEV  = 1 // develop environment
	EnvCodeTEST = 2 // test environment
	EnvCodePROD = 3 // production environment

	GenerateDataKey                     = "GenerateDataKey"
	GenerateDataKeyPair                 = "GenerateDataKeyPair"
	Encrypt                             = "Encrypt"
	Decrypt                             = "Decrypt"
	GetPublicKey                        = "GetPublicKey"
	DescribeKey                         = "DescribeKey"
	GenerateDataKeyWithoutPlaintext     = "GenerateDataKeyWithoutPlaintext"
	GenerateDataKeyPairWithoutPlaintext = "GenerateDataKeyPairWithoutPlaintext"
	Sign                                = "Sign"
	Verify                              = "Verify"
	ArnPrefix                           = "arn:euler:kms:bjht-1:"
	ArnInfix                            = ":key/"
)

var kmsUrl = map[int]string{
	EnvCodeDEV:  "http://kms-dev.wanyol.com",
	EnvCodeTEST: "http://kms-test.wanyol.com",
	EnvCodePROD: "http://datasec-kms-cn.oppo.local",
}

type KMSAPI interface {
	GenerateDataKey(input GenerateDataKeyInput) (*GenerateDataKeyOutput, error)
	GenerateDataKeyPair(input GenerateDataKeyPairInput) (*GenerateDataKeyPairOutput, error)
	Encrypt(input EncryptInput) (*EncryptOutput, error)
	Decrypt(input DecryptInput) (*DecryptOutput, error)
	GetPublicKey(input GetPublicKeyInput) (*GetPublicKeyOutput, error)
	DescribeKey(input DescribeKeyInput) (*DescribeKeyOutput, error)
	GenerateDataKeyWithoutPlaintext(input GenerateDataKeyWithoutPlaintextInput) (*GenerateDataKeyWithoutPlaintextOutput, error)
	GenerateDataKeyPairWithoutPlaintext(input GenerateDataKeyPairWithoutPlaintextInput) (*GenerateDataKeyPairWithoutPlaintextOutput, error)
	Sign(input SignInput) (*SignOutput, error)
	Verify(input VerifyInput) (*VerifyOutput, error)
}

// KMS provides the API operation methods for making requests to
// Key Management Service.
type KMSClient struct {
	AK      string
	SK      string
	request *Request
}

type KMSConfig struct {
	AK  string `json:"ak"`
	SK  string `json:"sk"`
	Url string `json:"url"`
}

func New(ak, sk string, envCode int) (*KMSClient, error) {
	err := errors.New("envCode is not standard")
	if envCode < EnvCodeDEV || envCode > EnvCodePROD {
		return nil, err
	}
	return NewWithUrl(ak, sk, kmsUrl[envCode])
}

// The user can customize the server address
func NewWithUrl(ak, sk, url string) (*KMSClient, error) {
	signer := NewSigner(ak, sk)
	request, err := newRequest(url, map[string]string{"Content-Type": "text/plain"}, signer)
	if err != nil {
		return nil, err
	}
	return &KMSClient{
		AK:      ak,
		SK:      sk,
		request: request,
	}, err
}

func NewWithConfig(conf KMSConfig) (*KMSClient, error) {
	signer := NewSigner(conf.AK, conf.SK)
	url := conf.Url
	if !strings.HasPrefix(url, "http://") {
		url = "http://" + url
	}
	request, err := newRequest(url, map[string]string{"Content-Type": "text/plain"}, signer)
	if err != nil {
		return nil, err
	}
	return &KMSClient{
		AK:      conf.AK,
		SK:      conf.SK,
		request: request,
	}, err
}

func getKeyIdFromArn(arn string) string {
	expr := fmt.Sprintf(`%s(\S*)%s(\S*)`, ArnPrefix, ArnInfix)
	re := regexp.MustCompile(expr)
	matched := re.FindStringSubmatch(arn)
	if len(matched) != 3 {
		return arn
	}
	return matched[2]
}

func IsValidCmkId(cmkId string) bool {
	keyId := getKeyIdFromArn(cmkId)
	if len(keyId) != 36 {
		return false
	}
	return true
}

// GenerateDataKey ...
func (c *KMSClient) GenerateDataKey(input GenerateDataKeyInput) (*GenerateDataKeyOutput, error) {
	if input.KeyId == "" {
		return nil, errors.New("KeyId is required")
	}
	keyId := getKeyIdFromArn(input.KeyId)
	if len(keyId) != 36 {
		return nil, errors.New("KeyId is not standard")
	}
	if input.KeySpec == "" && input.NumberOfBytes == 0 {
		return nil, errors.New("KeySpec and NumberOfBytes must have one")
	}
	if input.KeySpec != "" && input.NumberOfBytes != 0 {
		return nil, errors.New("Keyspec and numberofbytes have and can only have one")
	}

	if input.KeySpec != "" && input.KeySpec != "AES_128" && input.KeySpec != "AES_256" {
		return nil, errors.New("the KeySpec can only be 'AES_128' or 'AES_256'")
	}
	if input.NumberOfBytes != 0 && input.NumberOfBytes != 16 && input.NumberOfBytes != 32 {
		return nil, errors.New("the NumberOfBytes can only be '16' or '32'")
	}
	// response
	output := &GenerateDataKeyOutput{}

	if c.request == nil {
		return nil, ErrRequestInit
	}

	err := c.request.Send(GenerateDataKey, input, output, nil)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (c *KMSClient) GenerateDataKeyPair(input GenerateDataKeyPairInput) (*GenerateDataKeyPairOutput, error) {
	if input.KeyId == "" {
		return nil, errors.New("KeyId is required")
	}
	keyId := getKeyIdFromArn(input.KeyId)
	if len(keyId) != 36 {
		return nil, errors.New("KeyId is not standard")
	}
	if input.KeyPairSpec == "" {
		return nil, errors.New("KeyPairSpec is required")
	}
	switch input.KeyPairSpec {
	case "RSA_2048", "RSA_3072", "RSA_4096", "ECC_NIST_P256", "ECC_NIST_P384", "ECC_NIST_P521", "ECC_SECG_P256K1":
	default:
		return nil, errors.New("KeySpec is not standard")
	}
	output := &GenerateDataKeyPairOutput{}

	if c.request == nil {
		return nil, ErrRequestInit
	}
	err := c.request.Send(GenerateDataKeyPair, input, output, nil)
	if err != nil {
		return nil, err
	}

	return output, nil
}

func (c KMSClient) Encrypt(input EncryptInput) (*EncryptOutput, error) {
	if input.KeyId == "" {
		return nil, errors.New("KeyId is required")
	}
	keyId := getKeyIdFromArn(input.KeyId)
	if len(keyId) != 36 {
		return nil, errors.New("KeyId is not standard")
	}
	// response
	output := &EncryptOutput{}

	if c.request == nil {
		return nil, ErrRequestInit
	}
	err := c.request.Send(Encrypt, input, output, nil)
	if err != nil {
		return nil, err
	}

	return output, nil
}

func (c KMSClient) Decrypt(input DecryptInput) (*DecryptOutput, error) {
	if input.KeyId == "" {
		return nil, errors.New("KeyId is required")
	}
	keyId := getKeyIdFromArn(input.KeyId)
	if len(keyId) != 36 {
		return nil, errors.New("KeyId is not standard")
	}
	if len(input.CiphertextBlob) == 0 || len(input.CiphertextBlob) > 6144 {
		return nil, errors.New("CiphertextBlob is required and  cannot exceed 6144 in length")
	}
	if input.EncryptionAlgorithm == "" || len(input.EncryptionAlgorithm) > 6144 {
		return nil, errors.New("EncryptionAlgorithm is not standard")
	}
	if len(input.EncryptionContext) > 6144 {
		return nil, errors.New("EncryptionContext is not standard")
	}
	output := &DecryptOutput{}

	if c.request == nil {
		return nil, ErrRequestInit
	}
	err := c.request.Send(Decrypt, input, output, nil)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (c KMSClient) GetPublicKey(input GetPublicKeyInput) (*GetPublicKeyOutput, error) {
	if input.KeyId == "" {
		return nil, errors.New("KeyId is required")
	}
	keyId := getKeyIdFromArn(input.KeyId)
	if len(keyId) != 36 {
		return nil, errors.New("KeyId is not standard")
	}
	output := &GetPublicKeyOutput{}
	if c.request == nil {
		return nil, ErrRequestInit
	}
	err := c.request.Send(GetPublicKey, input, output, nil)
	if err != nil {
		return nil, err
	}

	return output, nil
}

func (c KMSClient) DescribeKey(input DescribeKeyInput) (*DescribeKeyOutput, error) {
	if input.KeyId == "" {
		return nil, errors.New("KeyId is required")
	}
	keyId := getKeyIdFromArn(input.KeyId)
	if len(keyId) != 36 {
		return nil, errors.New("KeyId is not standard")
	}
	output := &DescribeKeyOutput{}
	if c.request == nil {
		return nil, ErrRequestInit
	}
	err := c.request.Send(DescribeKey, input, output, nil)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (c KMSClient) GenerateDataKeyWithoutPlaintext(input GenerateDataKeyWithoutPlaintextInput) (*GenerateDataKeyWithoutPlaintextOutput, error) {
	if input.KeyId == "" {
		return nil, errors.New("KeyId is required")
	}
	keyId := getKeyIdFromArn(input.KeyId)
	if len(keyId) != 36 {
		return nil, errors.New("KeyId is not standard")
	}
	if input.KeySpec == "" && input.NumberOfBytes == 0 {
		return nil, errors.New("KeySpec and NumberOfBytes must have one")
	}
	if input.KeySpec != "" && input.NumberOfBytes != 0 {
		return nil, errors.New("Keyspec and numberofbytes have and can only have one")
	}

	if input.KeySpec != "" && input.KeySpec != "AES_128" && input.KeySpec != "AES_256" {
		return nil, errors.New("the KeySpec can only be 'AES_128' or 'AES_256'")
	}
	if input.NumberOfBytes != 0 && input.NumberOfBytes != 16 && input.NumberOfBytes != 32 {
		return nil, errors.New("the NumberOfBytes can only be '16' or '32'")
	}
	output := &GenerateDataKeyWithoutPlaintextOutput{}
	if c.request == nil {
		return nil, ErrRequestInit
	}
	err := c.request.Send(GenerateDataKeyWithoutPlaintext, input, output, nil)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (c KMSClient) GenerateDataKeyPairWithoutPlaintext(input GenerateDataKeyPairWithoutPlaintextInput) (*GenerateDataKeyPairWithoutPlaintextOutput, error) {
	if input.KeyId == "" {
		return nil, errors.New("KeyId is required")
	}
	keyId := getKeyIdFromArn(input.KeyId)
	if len(keyId) != 36 {
		return nil, errors.New("KeyId is not standard")
	}
	if input.KeyPairSpec == "" {
		return nil, errors.New("KeyPairSpec is required")
	}
	switch input.KeyPairSpec {
	case "RSA_2048", "RSA_3072", "RSA_4096", "ECC_NIST_P256", "ECC_NIST_P384", "ECC_NIST_P521", "ECC_SECG_P256K1":
	default:
		return nil, errors.New("KeySpec is not standard")
	}
	output := &GenerateDataKeyPairWithoutPlaintextOutput{}
	if c.request == nil {
		return nil, ErrRequestInit
	}
	err := c.request.Send(GenerateDataKeyPairWithoutPlaintext, input, output, nil)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (c KMSClient) Sign(input SignInput) (*SignOutput, error) {
	if input.KeyId == "" {
		return nil, errors.New("KeyId is required")
	}
	keyId := getKeyIdFromArn(input.KeyId)
	if len(keyId) != 36 {
		return nil, errors.New("KeyId is not standard")
	}
	if len(input.Message) == 0 {
		return nil, errors.New("Message is required")
	}
	if len(input.SigningAlgorithm) == 0 {
		return nil, errors.New("SigningAlgorithm is required")
	}
	decodeMessage, err := base64.StdEncoding.DecodeString(input.Message)
	if err != nil {
		return nil, errors.New("Message base64Decoding failed")
	}
	if len(decodeMessage) > 4096 {
		return nil, errors.New("Message maxlength is 4096")
	}
	messageType := input.MessageType
	if len(messageType) == 0 {
		messageType = "RAW"
	}
	if messageType != "RAW" && messageType != "DIGEST" {
		return nil, errors.New("MessageType is not standard")
	}
	switch input.SigningAlgorithm {
	case "RSASSA_PSS_SHA_256", "RSASSA_PSS_SHA_384", "RSASSA_PSS_SHA_512", "RSASSA_PKCS1_V1_5_SHA_256",
		"RSASSA_PKCS1_V1_5_SHA_384", "RSASSA_PKCS1_V1_5_SHA_512", "ECDSA_SHA_256", "ECDSA_SHA_384", "ECDSA_SHA_512":
	default:
		return nil, errors.New("SigningAlgorithm is not standard")
	}
	output := &SignOutput{}
	if c.request == nil {
		return nil, ErrRequestInit
	}
	err = c.request.Send(Sign, input, output, nil)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (c KMSClient) Verify(input VerifyInput) (*VerifyOutput, error) {
	if input.KeyId == "" {
		return nil, errors.New("KeyId is required")
	}
	keyId := getKeyIdFromArn(input.KeyId)
	if len(keyId) != 36 {
		return nil, errors.New("KeyId is not standard")
	}
	if len(input.Message) == 0 {
		return nil, errors.New("Message is required")
	}
	if len(input.SigningAlgorithm) == 0 {
		return nil, errors.New("SigningAlgorithm is required")
	}
	decodeMessage, err := base64.StdEncoding.DecodeString(input.Message)
	if err != nil {
		return nil, errors.New("Message base64Decoding failed")
	}
	if len(decodeMessage) > 4096 {
		return nil, errors.New("Message maxlength is 4096")
	}
	if len(input.Signature) == 0 {
		return nil, errors.New("Signature is required")
	}
	signature, err := base64.StdEncoding.DecodeString(input.Signature)
	if err != nil {
		return nil, errors.New("signature base64Decoding failed")
	}
	if len(signature) > 1024 {
		return nil, errors.New("Signature maxlength is 1024")
	}
	messageType := input.MessageType
	if len(messageType) == 0 {
		messageType = "RAW"
	}
	if messageType != "RAW" && messageType != "DIGEST" {
		return nil, errors.New("MessageType is not standard")
	}
	switch input.SigningAlgorithm {
	case "RSASSA_PSS_SHA_256", "RSASSA_PSS_SHA_384", "RSASSA_PSS_SHA_512", "RSASSA_PKCS1_V1_5_SHA_256",
		"RSASSA_PKCS1_V1_5_SHA_384", "RSASSA_PKCS1_V1_5_SHA_512", "ECDSA_SHA_256", "ECDSA_SHA_384", "ECDSA_SHA_512":
	default:
		return nil, errors.New("SigningAlgorithm is not standard")
	}
	output := &VerifyOutput{}
	if c.request == nil {
		return nil, ErrRequestInit
	}
	err = c.request.Send(Verify, input, output, nil)
	if err != nil {
		return nil, err
	}
	return output, nil
}
