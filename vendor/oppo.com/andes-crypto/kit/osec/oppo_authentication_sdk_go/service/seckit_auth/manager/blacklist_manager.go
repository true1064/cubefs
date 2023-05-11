package manager

import (
	"fmt"
	"sync"

	authClient "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/auth_client"

	"github.com/robfig/cron"
)

type BlackListManager struct {
	AuthClient     *authClient.AuthClient
	blackHostNames map[string]interface{}
	mux            sync.RWMutex
}

func (b *BlackListManager) Init() {
	b.updateBlackList()
	c := cron.New()
	spec := "0 */5 * * * ?"
	c.AddFunc(spec, func() {
		fmt.Println("cron running update blackList")
		b.updateBlackList()
	})
	c.Start()
}

func (b *BlackListManager) updateBlackList() {
	blackList, err := b.AuthClient.GetBlackList()
	if err != nil || blackList == nil {
		fmt.Println("blackList from auth server null")
		return
	}
	temp := make(map[string]interface{})
	if blackList.DisabledClients != nil {
		for _, v := range blackList.DisabledClients {
			temp[v.HostName] = nil
		}
	}
	b.mux.Lock()
	b.blackHostNames = temp
	b.mux.Unlock()
}

func (b *BlackListManager) IsHostNameInBlackList(hostName string) bool {
	res := false
	b.mux.RLock()
	if _, ok := b.blackHostNames[hostName]; ok {
		res = true
	}
	return res
}
