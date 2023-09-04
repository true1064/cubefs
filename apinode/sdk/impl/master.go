package impl

import (
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/sdk/master"
)

type MasterApi struct {
	//*master.MasterClient
	*master.AdminAPI
	*master.ClientAPI
}

func newSdkMasterCli(addr string) IMaster {
	masterCli := master.NewMasterClientFromString(addr, false)
	m := &MasterApi{
		AdminAPI:  masterCli.AdminAPI(),
		ClientAPI: masterCli.ClientAPI(),
	}

	return m
}

func masterToSdkErr(err error) error {
	if err == nil {
		return nil
	}

	if err == master.ErrNoValidMaster {
		return sdk.ErrNoMaster
	}

	return sdk.ErrInternalServerError
}
