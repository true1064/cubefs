package manager

import (
	"fmt"
	"sync"

	authClient "andescryptokit/osec/oppo_authentication_sdk_go/service/auth_client"
	authClientType "andescryptokit/osec/oppo_authentication_sdk_go/service/auth_client/types"

	"github.com/robfig/cron"
)

type SecpolicyManager struct {
	AuthClient *authClient.AuthClient
	SecPolicy  *authClientType.GetSecPolicyRespParam
	mux        sync.RWMutex
}

func (s *SecpolicyManager) Init() {
	s.getSecPolicy()
	c := cron.New()
	spec := "0 */5 * * * ?"
	c.AddFunc(spec, func() {
		fmt.Println("cron running update secpolicy")
		s.getSecPolicy()
	})
	c.Start()
}

func (s *SecpolicyManager) getSecPolicy() {
	respParam, err := s.AuthClient.GetSecPolicy()
	if err == nil {
		s.mux.Lock()
		s.SecPolicy = respParam
		s.mux.Unlock()
	} else {
		fmt.Println("get secpolicy error:", err.Error())
	}
}

func (s *SecpolicyManager) GetDetgadeSwitch() bool {
	res := true
	s.mux.RLock()
	if s.SecPolicy != nil {
		res = s.SecPolicy.DegradeEnable
	}
	s.mux.RUnlock()
	return res
}
