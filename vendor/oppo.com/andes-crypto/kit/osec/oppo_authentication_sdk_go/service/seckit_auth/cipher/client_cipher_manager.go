package cipher

import (
	manager "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/seckit_auth/manager"
	authType "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/seckit_auth/types"
)

type SecKitAuthClientCipherManager struct {
	CName                 string `json:"c_name"`
	SName                 string `json:"s_name"`
	AppEntityInfo         *authType.AppEntityInfo
	TicketToServerManager *manager.TicketManager
}

func (a *SecKitAuthClientCipherManager) Init() {
	a.TicketToServerManager.Init()
}

func (a *SecKitAuthClientCipherManager) GetCipher() *SecKitAuthClientRoundTripCipher {
	SecKitAuthClientRoundTripCipher := &SecKitAuthClientRoundTripCipher{
		TicketToServerManager: a.TicketToServerManager,
		CName:                 a.CName,
		SName:                 a.SName,
		AppEntityInfo:         a.AppEntityInfo,
	}
	return SecKitAuthClientRoundTripCipher
}
