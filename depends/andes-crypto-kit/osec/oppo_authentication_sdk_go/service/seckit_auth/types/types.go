package types

type AppEntityInfo struct {
	HostName string ` json:"host_name"`
}

type SessionKey struct {
	BeginTime int64  `json:"begin_time"`
	EndTime   int64  `json:"end_time"`
	MK        string `json:"mk"`
	EK        string `json:"ek"`
	SecLevel  int    `json:"sec_level"`
}

type WrapTicket struct {
	KeyVersion    int64  `json:"key_version"`
	TicketVersion int32  `json:"ticket_version"`
	Ticket        string `json:"ticket"`
}

type Kek struct {
	Version    int64  `json:"version"`
	Kek        string `json:"kek"`
	ExpireTime uint64 `json:"expire_time"`
}

type TicketKeeper struct {
	SessionKey *SessionKey
	Ticket     string
}

type Authenticator struct {
	CName      string         `json:"c_name"`
	EntityInfo *AppEntityInfo `json:"entity_info"`
}

type Ticket struct {
	Version        int64          `json:"version"`
	Usage          int32          `json:"usage"`
	CName          string         `json:"c_name"`
	SName          string         `json:"s_name"`
	BeginTime      int64          `json:"begin_time"`
	EndTime        int64          `json:"end_time"`
	MacKey         string         `json:"mac_key"`
	EncKey         string         `json:"enc_key"`
	Oob            string         `json:"oob"`
	EntityInfo     *AppEntityInfo `json:"entity_info"`
	PermissionInfo *Permission    `json:"permission_info"`
	SecLevel       int            `json:"sec_level"`
}

type Permission struct {
	PermissionLocation int               `json:"permission_location"`
	Items              []*PermissionItem `json:"items"`
	PermissionLevel    int               `json:"permission_level"`
}

type PermissionItem struct {
	ID             int64    `json:"id"`
	ResourceAction string   `json:"resource_action"`
	Resources      []string `json:"resources"`
}
