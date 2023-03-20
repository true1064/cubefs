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
	*master.MasterClient
}

func (m *MasterApi) GetClusterIP() (cp *proto.ClusterIP, err error) {
	cp, err = m.AdminAPI().GetClusterIP()
	return cp, masterToSdkErr(err)
}

func (m *MasterApi) ListVols(keywords string) (volsInfo []*proto.VolInfo, err error) {
	volsInfo, err = m.AdminAPI().ListVols("")
	return volsInfo, masterToSdkErr(err)
}

func newSdkMasterCli(addr string) IMaster {
	m := &MasterApi{
		master.NewMasterClientFromString(addr, false),
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
