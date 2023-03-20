package sdk

import (
	"context"
	"sync"
)

type ClusterManager interface {
	// AddCluster masterAddr, eg: host1:port,host2:port
	AddCluster(ctx context.Context, clusterId string, masterAddr string) error
	// GetCluster if not exist, return nil
	GetCluster(clusterId string) ICluster
	ListCluster() []ICluster
}

type clusterMgr struct {
	clk        sync.RWMutex
	clusterMap map[string]ICluster

	create func(context context.Context, cId, addr string) (ICluster, error)
}

func NewClusterMgr() ClusterManager {
	cm := &clusterMgr{
		clusterMap: make(map[string]ICluster),
	}

	cm.create = newCluster
	return cm
}

func (cm *clusterMgr) ListCluster() []ICluster {
	arr := make([]ICluster, 0, len(cm.clusterMap))
	cm.clk.RLock()
	defer cm.clk.RUnlock()

	for _, v := range cm.clusterMap {
		arr = append(arr, v)
	}

	return arr
}

func (cm *clusterMgr) getCluster(cId string) ICluster {
	cm.clk.RLock()
	defer cm.clk.RUnlock()

	return cm.clusterMap[cId]
}

func (cm *clusterMgr) putCluster(cId string, newC ICluster) {
	cm.clk.Lock()
	defer cm.clk.Unlock()

	cm.clusterMap[cId] = newC
}

func (cm *clusterMgr) AddCluster(ctx context.Context, cId string, masterAddr string) error {
	// check if cluster exist
	c := cm.getCluster(cId)
	if c != nil {
		if c.addr() == masterAddr {
			return nil
		}
		// update masterAddr
		return c.updateAddr(ctx, masterAddr)
	}

	c, err := cm.create(ctx, cId, masterAddr)
	if err != nil {
		return err
	}

	cm.putCluster(cId, c)
	return nil
}

func (cm *clusterMgr) GetCluster(cId string) ICluster {
	return cm.getCluster(cId)
}
