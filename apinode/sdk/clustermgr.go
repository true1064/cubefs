package sdk

import (
	"context"
)

type ICluster interface {
	GetVol(name string) IVolume
	// ListVols caller should cache result
	ListVols() []*VolInfo
	Info() *ClusterInfo
	Addr() string
	UpdateAddr(ctx context.Context, addr string) error
	AllocFileId(ctx context.Context) (id uint64, err error)
}

type ClusterManager interface {
	// AddCluster masterAddr, eg: host1:port,host2:port
	AddCluster(ctx context.Context, clusterId string, masterAddr string) error
	// GetCluster if not exist, return nil
	GetCluster(clusterId string) ICluster
	// ListCluster caller should cache result
	ListCluster() []*ClusterInfo
}
