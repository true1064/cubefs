package manager

import (
	"errors"
	"fmt"
	"sync"

	authClient "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/auth_client"
	authType "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/seckit_auth/types"

	"github.com/robfig/cron"
)

type KekManager struct {
	AuthClient *authClient.AuthClient
	KekMap     map[int64]*authType.Kek
	mux        sync.RWMutex
}

func (k *KekManager) Init() {
	k.updateKek()
	c := cron.New()
	spec := "0 */5 * * * ?"
	c.AddFunc(spec, func() {
		fmt.Println("cron running update kek")
		k.updateKek()
	})
	c.Start()
}

func (k *KekManager) getKekFromServer() (map[int64]*authType.Kek, error) {
	respParam, err := k.AuthClient.GetKeks()
	if err != nil {
		return nil, err
	}
	kekMap := make(map[int64]*authType.Kek, len(respParam.Keks))
	for _, k := range respParam.Keks {
		kekMap[k.Version] = k
	}
	return kekMap, nil
}

func (k *KekManager) GetKek(version int64) (*authType.Kek, error) {
	kek := &authType.Kek{}
	k.mux.RLock()
	if k.KekMap != nil {
		kek = k.KekMap[version]
	}
	k.mux.RUnlock()
	if kek == nil {
		k.mux.Lock()
		if k.KekMap == nil || k.KekMap[version] == nil {
			m, err := k.getKekFromServer()
			if err != nil {
				return nil, err
			}
			k.KekMap = m
			kek = k.KekMap[version]
			if kek == nil {
				return nil, errors.New("no kek found")
			}
		}
		k.mux.Unlock()
	}
	return kek, nil
}

func (k *KekManager) updateKek() {
	kMap, err := k.getKekFromServer()
	fmt.Println(err)
	k.mux.Lock()
	k.KekMap = kMap
	k.mux.Unlock()
}
