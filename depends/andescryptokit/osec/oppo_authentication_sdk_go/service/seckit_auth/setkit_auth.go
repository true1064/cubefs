package auth_kit

import (
	"errors"
	"os"
	"regexp"
	"strings"

	authClient "andescryptokit/osec/oppo_authentication_sdk_go/service/auth_client"
	cipher "andescryptokit/osec/oppo_authentication_sdk_go/service/seckit_auth/cipher"
	manager "andescryptokit/osec/oppo_authentication_sdk_go/service/seckit_auth/manager"
	authType "andescryptokit/osec/oppo_authentication_sdk_go/service/seckit_auth/types"
)

type SecKitAuth struct {
	AppName              string                  `json:"appName"`
	HostName             string                  `json:"hostName"`
	AuthUrl              string                  `json:"authUrl"`
	AK                   string                  `json:"ak"`
	SK                   string                  `json:"sk"`
	AuthClient           *authClient.AuthClient  `json:"AuthClient"`
	AppEntityInfo        *authType.AppEntityInfo `json:"appEntityInfo"`
	ClientCipherManagers map[string]*cipher.SecKitAuthClientCipherManager
}

func GetInstance(appName, authUrl, ak, sk string) (*SecKitAuth, error) {
	hostName, errOs := os.Hostname()
	if errOs != nil {
		return nil, errors.New(authType.AUTH_ERR_AUTH_INVALID_PARAM_FORMAT)
	}
	match, _ := regexp.MatchString("(.*)\\.(.*)\\.(.*)", hostName)
	if !match {
		hostName = hostName + "." + appName + "." + "unknown"
	}
	hostNameArray := strings.Split(hostName, ".")
	if !strings.EqualFold(hostNameArray[1], appName) || strings.EqualFold(ak, "") || strings.EqualFold(sk, "") {
		return nil, errors.New(authType.AUTH_ERR_AUTH_INVALID_PARAM_FORMAT)
	}

	appEntityInfo := &authType.AppEntityInfo{
		HostName: hostName,
	}
	clientAppProxyMap := make(map[string]*cipher.SecKitAuthClientCipherManager)
	AuthKit := &SecKitAuth{
		AppName:       appName,
		AuthUrl:       authUrl,
		AK:            ak,
		SK:            sk,
		HostName:      hostName,
		AppEntityInfo: appEntityInfo,
		AuthClient: &authClient.AuthClient{
			AppName:       appName,
			AuthUrl:       authUrl,
			AK:            ak,
			SK:            sk,
			AppEntityInfo: appEntityInfo,
		},
		ClientCipherManagers: clientAppProxyMap,
	}
	err := AuthKit.AuthClient.Init()
	return AuthKit, err
}

func (a *SecKitAuth) GetClientCipherManager(sName string) (*cipher.SecKitAuthClientCipherManager, error) {
	if strings.EqualFold(sName, a.AppName) {
		return nil, errors.New(authType.AUTH_ERR_SERVER_NAME_SAME_WITH)
	}
	proxy := a.ClientCipherManagers[sName]
	if proxy == nil {
		ticketManager := &manager.TicketManager{
			SName:      sName,
			AuthClient: a.AuthClient,
		}
		proxy = &cipher.SecKitAuthClientCipherManager{
			CName:                 a.AppName,
			SName:                 sName,
			AppEntityInfo:         a.AppEntityInfo,
			TicketToServerManager: ticketManager,
		}
		proxy.Init()
		a.ClientCipherManagers[sName] = proxy
	}
	return proxy, nil
}

func (a *SecKitAuth) GetServiceCipherManager() *cipher.SecKitAuthServiceCipherManager {
	kekManager := &manager.KekManager{
		AuthClient: a.AuthClient,
	}
	blackListManager := &manager.BlackListManager{
		AuthClient: a.AuthClient,
	}
	permissionManager := &manager.PermissionManager{
		AuthClient: a.AuthClient,
	}
	secpolicyManager := &manager.SecpolicyManager{
		AuthClient: a.AuthClient,
	}
	proxy := &cipher.SecKitAuthServiceCipherManager{
		AppName:           a.AppName,
		AuthClient:        a.AuthClient,
		KekManager:        kekManager,
		BlackListManager:  blackListManager,
		PermissionManager: permissionManager,
		SecpolicyManager:  secpolicyManager,
	}
	proxy.Init()
	return proxy
}
