package sdk

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
)

type IMaster interface {
	AdminApi
}

type AdminApi interface {
	GetClusterIP() (cp *proto.ClusterIP, err error)
	ListVols(keywords string) (volsInfo []*proto.VolInfo, err error)
}

type MasterApi struct {
	//*master.MasterClient
	*master.AdminAPI
}

func newSdkMasterCli(addr string) IMaster {
	masterCli := master.NewMasterClientFromString(addr, false)
	m := &MasterApi{
		masterCli.AdminAPI(),
	}

	return m
}

func masterToSdkErr(err error) error {
	if err == nil {
		return nil
	}

	if err == master.ErrNoValidMaster {
		return ErrNoMaster
	}

	return ErrInternalServerError
}
