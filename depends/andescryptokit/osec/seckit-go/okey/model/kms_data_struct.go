package model

/*************************************** 通用结构 ********************************************/
type CMSEncryptedData struct {
	Alg           string `json:"alg"`
	Padding       string `json:"padding"`
	Iv            string `json:"iv"`
	Aad           string `json:"aad"`
	Tag           string `json:"tag"`
	EncryptedData string `json:"encrypted_data"`
}

type CMSSignedData struct {
	SignAlg       string `json:"sign_alg"`
	HashId        string `json:"hash_id"`
	SignedContent string `json:"signed_content"`
}

type KMSError struct {
	RequestId string `json:"request_id"`
	Code      int32  `json:"code"`
	Message   string `json:"message"`
}

type Header struct {
	RequestId  string      `json:"request_id"`
	Version    int32       `json:"version"`
	EncType    string      `json:"enc_type"`
	SigType    string      `json:"sig_type"`
	KeyType    string      `json:"key_type"`
	WbKeyIndex *WBKeyIndex `json:"wb_key_index"`
	UakIndex   *UAKIndex   `json:"uak_index"`
	Username   string      `json:"username"`
	Timestamp  uint64      `json:"timestamp"`
	Nonce      string      `json:"nonce"`
	Ticket     string      `json:"ticket"` // base64
}

type Pack struct {
	Header    string `json:"header"`    // json str
	Payload   string `json:"payload"`   // json str
	Signature string `json:"signature"` // base64 str
}

type UAKIndex struct {
	AppName string `json:"app_name"`
	UakId   string `json:"uak_id"`
}

type WBKeyIndex struct {
	AppName   string `json:"app_name"`
	WbId      string `json:"wb_id"`
	WbVersion int32  `json:"wb_version"`
	KeyId     string `json:"key_id"`
}

// SecurityPolicy ...
type SecurityPolicy struct {
	DegradeFlag       bool `json:"degrade_flag"`
	TicketDegradeFlag bool `json:"ticket_degrade_flag"`
	TicketExpired     bool `json:"ticket_expired"`
}

// EnvelopePack ...
type EnvelopePack struct {
	App          string   `json:"app"`
	ParsePayload string   `json:"parse_payload"`
	CMS          Envelope `json:"cms"`
}

// Envelope ...
type Envelope struct {
	Version  string  `json:"version"`
	WorkMode string  `json:"work_mode"`
	AppName  string  `json:"appName"`
	Pk       PK      `json:"pk"`
	Dek      DEK     `json:"dek"`
	Payload  Payload `json:"payload"`
}

// PK ...
type PK struct {
	PkType      string `json:"pk_type"`
	PkBitLength int    `json:"pk_bit_length"`
	PkHashValue string `json:"pk_hash_value"`
}

// DEK ...
type DEK struct {
	DekEncCipherSuite string `json:"dec_enc_cipher_suite"`
	DekBitLength      int    `json:"dek_bit_length"`
	DekPlainHashValue string `json:"dek_plain_hash_value"`
	EncryptedDek      string `json:"encrypted_dek"`
}

// Payload ...
type Payload struct {
	PayloadCipherSuite   string `json:"payload_cipher_suite"`
	PayloadIv            string `json:"payload_iv"`
	PayloadAad           string `json:"payload_aad"`
	PayloadTag           string `json:"payload_tag"`
	EncryptedPayload     string `json:"encrypted_payload"`
	EncryptedPayloadHmac string `json:"encrypted_payload_hmac"`
}
