package model

/*************************************** 通用结构 ********************************************/

// ProtoPack ...
type ProtoPack struct {
	AuthType    string `json:"type"`
	AuthVersion int    `json:"auth_version"`
	RequestID   string `json:"request_id"`
	Payload     string `json:"auth_cipher"` // CmdPack payload after enrypt and sign
}

// CmdPack ...
type CmdPack struct {
	Action string `json:"action"`
	Param  string `json:"params"`
}

// KeyPair ...
type KeyPair struct {
	PKHash string `json:"pk_hash"`
	PK     string `json:"pk"`
	SK     string `json:"sk"`
}

// AppKeyPairInfo ...
type AppKeyPairInfo struct {
	AppName  string    `json:"app_name"`
	KeyPairs []KeyPair `json:"key_pairs"`
}

// WBKey ...
type WBKey struct {
	KeyId string `json:"key_id"`
	Key   string `json:"key"`
}

// WBs ...
type WBs struct {
	WbId  string  `json:"wb_id"`
	WbKey []WBKey `json:"wb_key"`
}

// AppSecInfo ...
type AppSecInfo struct {
	AppName              string `json:"app_name"`
	Kek                  string `json:"kek"`
	AllowDegradation     bool   `json:"allow_degradation"`
	AllowUnauthenticated bool   `json:"allow_unauthenticated"`
	Wbs                  []WBs  `json:"wbs"`
}

/*************************************** 公共接口 ********************************************/

/*************************************** 终端交互接口 ********************************************/
type ReqGetKMSSystemTime struct {
	Account string `json:"account"`
}

type ResGetKMSSystemTime struct {
	Timestamp uint64 `json:"timestamp"`
}

type ReqGetKMSTicket struct {
	Account string `json:"account"`
}

type ResGetKMSTicket struct {
	Account   string `json:"account"`
	BeginTime uint64 `json:"begin_time"`
	EndTime   uint64 `json:"end_time"`
	Mk        string `json:"mk"`     // base64
	Dek       string `json:"dek"`    // base64
	Ticket    string `json:"ticket"` // base64
}

type ReqGetServiceTicket struct {
	AppName  string      `json:"app_name"`
	DeviceId string      `json:"device_id"`
	KeyStats []*KeyStats `json:"key_stats"`
}

type KeyStats struct {
	KeyId        string `json:"key_id"`
	AppName      string `json:"app_name"`
	WbId         string `json:"wb_id"`
	DecryptCount uint64 `json:"decrypt_count"`
	EncryptCount uint64 `json:"encrypt_count"`
}

type ResGetServiceTicket struct {
	AppName   string `json:"app_name"`
	BeginTime uint64 `json:"begin_time"`
	EndTime   uint64 `json:"end_time"`
	Mk        string `json:"mk"`     // base64
	Dek       string `json:"dek"`    // base64
	Ticket    string `json:"ticket"` // base64
}

type Ticket struct {
	Version    int32  `json:"version"`
	Ctype      string `json:"ctype"`
	Cname      string `json:"cname"`
	Stype      string `json:"stype"`
	Sname      string `json:"sname"`
	BeginTime  uint64 `json:"begin_time"`
	EndTime    uint64 `json:"end_time"`
	Mk         string `json:"mk"`
	Dek        string `json:"dek"`
	TicketType int32  `json:"ticket_type"`
	Oob        string `json:"oob"`
}

type ReqGetKMSCertificate struct {
	AppName  string `json:"app_name"`
	DeviceId string `json:"device_id"`
}

type ResGetKMSCertificate struct {
	AppName      string   `json:"app_name"`
	KmsCertChain []string `json:"kms_cert_chain"`
}

/*************************************** 业务后台交互接口 ********************************************/

// ReqDownloadKeyPair ...
type ReqDownloadKeyPair struct {
	TargetAppnames []string `json:"target_app_names"`
}

// ResDownloadKeyPair ...
type ResDownloadKeyPair struct {
	AppKeyPairInfos []AppKeyPairInfo `json:"app_keypair_info"`
}

// ReqGetKEK ...
type ReqGetKEK struct {
	AppName       string `json:"app_name"`
	TargetAppName string `json:"target_app_name"`
}

// ResGetKEK ...
type ResGetKEK struct {
	AppName string `json:"app_name"`
	Kek     string `json:"kek"`
}

// ReqDownloadWBKeys ...
type ReqDownloadWBKeys struct {
	AppName        string `json:"app_name"`
	TargetAppName  string `json:"target_app_name"`
	LastAccessTime uint64 `json:"last_access_time"`
}

// ResDownloadWBKeys ...
type ResDownloadWBKeys struct {
	AppName string `json:"app_name"`
	Wbs     []WBs  `json:"wbs"`
}

// ReqGetGrantedAppName ...
type ReqGetGrantedAppName struct{}

// ResGetGrantedAppName ...
type ResGetGrantedAppName struct {
	TargetAppNames []string `json:"target_app_names"`
	Result         string   `json:"result"`
}

// ReqGetGrantedAppInfo ...
type ReqGetGrantedAppInfo struct {
	TargetAppNames []string `json:"target_app_names"`
}

// ResGetGrantedAppInfo ...
type ResGetGrantedAppInfo struct {
	AppInfos []AppSecInfo `json:"app_infos"`
	Result   string       `json:"result"`
}

// ReqGetSecurityPolicy ...
type ReqGetSecurityPolicy struct {
	AppName       string `json:"app_name"`
	TargetAppName string `json:"target_app_name"`
}

// ResGetSecurityPolicy ...
type ResGetSecurityPolicy struct {
	AppName              string `json:"app_name"`
	AllowDegradation     bool   `json:"allow_degradation"`
	AllowUnauthenticated bool   `json:"allow_unauthenticated"`
}

// ReqReportWBUsageStats ...
type ReqReportWBUsageStats struct {
	AppName  string     `json:"app_name"`
	KeyStats []KeyStats `json:"key_stats"`
}

// ResReportWBUsageStats ...
type ResReportWBUsageStats struct {
	Result string `json:"result"`
}
