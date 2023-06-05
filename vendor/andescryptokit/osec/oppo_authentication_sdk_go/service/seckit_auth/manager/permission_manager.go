package manager

import (
	"fmt"
	"strings"
	"sync"

	authClient "andescryptokit/osec/oppo_authentication_sdk_go/service/auth_client"
	authClientType "andescryptokit/osec/oppo_authentication_sdk_go/service/auth_client/types"

	"github.com/robfig/cron"
)

type PermissionManager struct {
	AuthClient  *authClient.AuthClient
	Permissions map[string]*authClientType.AppPermission
	mux         sync.RWMutex
}

const MAX_ITEMS_PER_PULL = 10

func (p *PermissionManager) Init() {
	p.pullPermissions()
	c := cron.New()
	spec := "0 */5 * * * ?"
	c.AddFunc(spec, func() {
		fmt.Println("cron running update permission")
		p.pullPermissions()
	})
	c.Start()
}

func (p *PermissionManager) pullPermissions() {
	respParam, err := p.AuthClient.GetPermittedClients()
	if err != nil {
		fmt.Println("get permittesClients error:", err.Error())
		return
	}
	totalPermittesClients := 0
	if respParam.PermittedClientList != nil {
		totalPermittesClients = len(respParam.PermittedClientList)
	}
	if totalPermittesClients == 0 {
		p.Permissions = make(map[string]*authClientType.AppPermission, 0)
	}
	pullTimes := totalPermittesClients / MAX_ITEMS_PER_PULL
	if totalPermittesClients%MAX_ITEMS_PER_PULL != 0 {
		pullTimes = totalPermittesClients/MAX_ITEMS_PER_PULL + 1
	}

	tempMap := make(map[string]*authClientType.AppPermission)
	for i := 0; i < pullTimes; i++ {
		appNames := []string{}
		startIndex := i * MAX_ITEMS_PER_PULL
		endIndex := startIndex + MAX_ITEMS_PER_PULL
		if endIndex > totalPermittesClients {
			endIndex = totalPermittesClients
		}
		for j := startIndex; j < endIndex; j++ {
			appNames = append(appNames, respParam.PermittedClientList[j].AppName)
		}
		getPermissionsreq := &authClientType.GetPermissionsReqParam{
			AppNameList: appNames,
		}
		permissionsResp, err := p.AuthClient.GetPermissions(getPermissionsreq)
		if err != nil {
			fmt.Println("get permissions error:", err.Error())
			return
		}
		for _, v := range permissionsResp.AppPermissions {
			tempMap[v.AppName] = v
		}
	}

	p.mux.Lock()
	p.Permissions = tempMap
	p.mux.Unlock()
}

func (p *PermissionManager) getClientsPermissions(clientApp string) *authClientType.AppPermission {
	res := &authClientType.AppPermission{}
	p.mux.RLock()
	if p.Permissions != nil {
		res = p.Permissions[clientApp]
	}
	p.mux.RUnlock()
	return res
}

func (p *PermissionManager) IsActionAccessPermitted(clientApp, reqAction string) bool {
	targetPermission := p.getClientsPermissions(clientApp)
	if targetPermission == nil {
		return false
	}
	if targetPermission.PermissionLevel == authClientType.APPLICATION {
		return true
	}
	for _, v := range targetPermission.PermissionItemList {
		if strings.EqualFold(v.ResourceAction, reqAction) {
			return true
		}
	}
	return false
}

func (p *PermissionManager) IsResourceAccessPermitted(clientApp, reqAction, reqResource string) bool {
	targetPermission := p.getClientsPermissions(clientApp)
	if targetPermission == nil {
		return false
	}
	if targetPermission.PermissionLevel == authClientType.APPLICATION {
		return true
	}
	for _, v := range targetPermission.PermissionItemList {
		if strings.EqualFold(v.ResourceAction, reqAction) {
			for _, item := range v.Resources {
				if strings.EqualFold(reqResource, item) {
					return true
				}
			}
		}
	}
	return false
}
