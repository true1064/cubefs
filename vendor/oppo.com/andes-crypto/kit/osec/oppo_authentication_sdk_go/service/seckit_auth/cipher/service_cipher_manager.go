package cipher

import (
	authClient "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/auth_client"
	manager "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/seckit_auth/manager"
)

type SecKitAuthServiceCipherManager struct {
	AppName           string `json:"appName"`
	AuthClient        *authClient.AuthClient
	KekManager        *manager.KekManager
	PermissionManager *manager.PermissionManager
	BlackListManager  *manager.BlackListManager
	SecpolicyManager  *manager.SecpolicyManager
}

func (a *SecKitAuthServiceCipherManager) Init() {
	a.BlackListManager.Init()
	a.PermissionManager.Init()
	a.KekManager.Init()
	a.SecpolicyManager.Init()
}

func (a *SecKitAuthServiceCipherManager) GetCipher() *SecKitAuthServiceRoundTripCipher {
	SecKitAuthServiceRoundTripCipher := &SecKitAuthServiceRoundTripCipher{
		SName:             a.AppName,
		BlackListManager:  a.BlackListManager,
		PermissionManager: a.PermissionManager,
		KekManager:        a.KekManager,
		SecpolicyManager:  a.SecpolicyManager,
		AuthClient:        a.AuthClient,
		DegradeSwitch:     true,
		Unseal:            false,
	}
	return SecKitAuthServiceRoundTripCipher
}
