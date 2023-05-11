package manager

import (
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron"

	authClient "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/auth_client"
	authType "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/seckit_auth/types"
)

type TicketManager struct {
	SName          string `json:"s_name"`
	AuthClient     *authClient.AuthClient
	TickerKeeper   *authType.TicketKeeper
	HavePermission bool
	mux            sync.RWMutex
	delayTime      int64
}

func (t *TicketManager) Init() {
	t.GetOncePeerTicket()
	go func() {
		t.UpdatePeerTicket()
	}()
}

func (t *TicketManager) GetPeerTicket() *authType.TicketKeeper {
	keeper := &authType.TicketKeeper{}
	t.mux.RLock()
	if t.TickerKeeper != nil {
		keeper = t.TickerKeeper
	}
	t.mux.RUnlock()

	timeNow := time.Now().Unix()

	if keeper == nil || keeper.SessionKey.EndTime-60 < timeNow {
		fmt.Printf("currenttime: %v, TicketManager.GetPeerTicket() keeper nil or sessioneky time(%v) invalid\n", time.Now().Format("2006-01-02 15:04:05"), keeper.SessionKey.EndTime)
		keeper = nil
	}
	// fmt.Printf("keeper valid\n")

	return keeper
}

func (t *TicketManager) GetOncePeerTicket() {
	// fmt.Printf("currenttime: %v, GetOncePeerTicket() authticket: %s\n", time.Now().Format("2006-01-02 15:04:05"), t.AuthClient.TicketKeeper.Ticket)
	// if t.TickerKeeper != nil {
	// 	fmt.Printf("cuurenttime: %v, GetOncePeerTicket() peerticket: %s\n", time.Now().Format("2006-01-02 15:04:05"), t.TickerKeeper.Ticket)
	// } else {
	// 	fmt.Printf("currenttime: %v, GetOncePeerTicket() peerticket: nil\n", time.Now().Format("2006-01-02 15:04:05"))
	// }
	keeper, err := t.AuthClient.GetPeerTicket(t.SName)
	if err != nil {
		t.HavePermission = false
		t.delayTime = 60
		return
	}
	timeNow := time.Now().Unix()

	if keeper.SessionKey.EndTime < timeNow+300 {
		t.HavePermission = false
		t.delayTime = 6
		return
	}
	t.delayTime = keeper.SessionKey.EndTime - timeNow - 300
	t.mux.Lock()
	t.TickerKeeper = keeper
	t.HavePermission = true
	t.mux.Unlock()

	// fmt.Printf("currenttime: %v, ticket manager: delaytime(%v)  ", time.Now().Format("2006-01-02 15:04:05"), t.delayTime)
	// fmt.Printf("currenttime: %v, TicketManager.GetOncePeerTicket() sessioneky time(%v)\n", time.Now().Format("2006-01-02 15:04:05"), keeper.SessionKey.EndTime)
}

func (t *TicketManager) UpdatePeerTicket() {
	// time.Sleep(time.Second * time.Duration(t.delayTime))
	// t.GetOncePeerTicket()

	t2 := cron.New()
	min2 := 5
	spec2 := fmt.Sprintf("0 */%d * * * ?", min2)
	t2.AddFunc(spec2, func() {
		t.GetOncePeerTicket()
	})
	t2.Start()
}
