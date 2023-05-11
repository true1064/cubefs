package kms

type GenerateDataKeyInput struct {
	KeyId             string `json:"key_id"`
	EncryptionContext string `json:"encryption_context"`
	KeySpec           string `json:"key_spec"`
	NumberOfBytes     int32  `json:"number_of_bytes"`
}

type GenerateDataKeyOutput struct {
	CiphertextBlob string `json:"ciphertext_blob"`
	KeyId          string `json:"key_id"`
	Plaintext      string `json:"plaintext"`
}

type GenerateDataKeyPairInput struct {
	EncryptionContext string `json:"encryption_context"`
	KeyId             string `json:"key_id"`
	KeyPairSpec       string `json:"key_pair_spec"`
}

type GenerateDataKeyPairOutput struct {
	KeyId                    string `json:"key_id"`
	KeyPairSpec              string `json:"key_pair_spec"`
	PrivateKeyCiphertextBlob string `json:"private_key_ciphertext_blob"`
	PrivateKeyPlaintext      string `json:"private_key_plaintext"`
	PublicKey                string `json:"public_key"`
}

type EncryptInput struct {
	EncryptionAlgorithm string `json:"encryption_algorithm"`
	EncryptionContext   string `json:"encryption_context"`
	KeyId               string `json:"key_id"`
	Plaintext           string `json:"plaintext"`
}

type EncryptOutput struct {
	CiphertextBlob      string `json:"ciphertext_blob"`
	EncryptionAlgorithm string `json:"encryption_algorithm"`
	KeyId               string `json:"key_id"`
}

type DecryptInput struct {
	CiphertextBlob      string `json:"ciphertext_blob"`
	EncryptionAlgorithm string `json:"encryption_algorithm"`
	EncryptionContext   string `json:"encryption_context"`
	KeyId               string `json:"key_id"`
}

type DecryptOutput struct {
	EncryptionAlgorithm string `json:"encryption_algorithm"`
	KeyId               string `json:"key_id"`
	Plaintext           string `json:"plaintext"`
}

type GetPublicKeyInput struct {
	KeyId string `json:"key_id"`
}

type GetPublicKeyOutput struct {
	CustomerMasterKeySpec string   `json:"customer_master_key_spec"`
	EncryptionAlgorithms  []string `json:"encryption_algorithms"`
	KeyId                 string   `json:"key_id"`
	KeyUsage              string   `json:"key_usage"`
	PublicKey             string   `json:"public_key"`
	SigningAlgorithms     []string `json:"signing_algorithms"`
}

type DescribeKeyInput struct {
	KeyId string `json:"key_id"`
}

type DescribeKeyOutput struct {
	Arn                   string   `json:"arn"`
	AccountId             string   `json:"account_id"`
	CreationTime          string   `json:"creation_time"`
	CustomerMasterKeySpec string   `json:"customer_master_key_spec"`
	DeletionDate          string   `json:"deletion_date"`
	Description           string   `json:"description"`
	Enabled               bool     `json:"enabled"`
	EncryptionAlgorithms  []string `json:"encryption_algorithms"`
	ExpirationModel       string   `json:"expiration_model"`
	KeyId                 string   `json:"key_id"`
	KeyState              string   `json:"key_state"`
	KeyUsage              string   `json:"key_usage"`
	Origin                string   `json:"origin"`
	SigningAlgorithms     []string `json:"signing_algorithms"`
	ValidTo               string   `json:"valid_to"`
	Alias                 string   `json:"alias"`
	Keytype               string   `json:"keytype"`
}

type GenerateDataKeyWithoutPlaintextInput struct {
	KeyId             string `json:"key_id"`
	EncryptionContext string `json:"encryption_context"`
	KeySpec           string `json:"key_spec"`
	NumberOfBytes     int    `json:"number_of_bytes"`
}

type GenerateDataKeyWithoutPlaintextOutput struct {
	CiphertextBlob string `json:"ciphertext_blob"`
	KeyId          string `json:"key_id"`
}

type GenerateDataKeyPairWithoutPlaintextInput struct {
	EncryptionContext string `json:"encryption_context"`
	KeyId             string `json:"key_id"`
	KeyPairSpec       string `json:"key_pair_spec"`
}

type GenerateDataKeyPairWithoutPlaintextOutput struct {
	KeyId                    string `json:"key_id"`
	KeyPairSpec              string `json:"key_pair_spec"`
	PrivateKeyCiphertextBlob string `json:"private_key_ciphertext_blob"`
	PublicKey                string `json:"public_key"`
}

type SignInput struct {
	KeyId            string `json:"key_id"`
	Message          string `json:"message"`
	MessageType      string `json:"message_type"`
	SigningAlgorithm string `json:"signing_algorithm"`
}

type SignOutput struct {
	KeyId                    string `json:"key_id"`
	KeyPairSpec              string `json:"key_pair_spec"`
	PrivateKeyCiphertextBlob string `json:"private_key_ciphertext_blob"`
	PublicKey                string `json:"public_key"`
}

type VerifyInput struct {
	KeyId            string `json:"key_id"`
	Message          string `json:"message"`
	MessageType      string `json:"message_type"`
	Signature        string `json:"signature"`
	SigningAlgorithm string `json:"signing_algorithm"`
}

type VerifyOutput struct {
	KeyId            string `json:"key_id"`
	SignatureValid   bool   `json:"signature_valid"`
	SigningAlgorithm string `json:"signing_algorithm"`
}
