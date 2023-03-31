package impl

import (
	"context"
	"sync"

	"github.com/cubefs/cubefs/apinode/sdk"
)

type clusterMgr struct {
	clk        sync.RWMutex
	clusterMap map[string]sdk.ICluster

	create func(context context.Context, cId, addr string) (sdk.ICluster, error)
}

func NewClusterMgr() sdk.ClusterManager {
	return newClusterMgr()
}

func newClusterMgr() *clusterMgr {
	cm := &clusterMgr{
		clusterMap: make(map[string]sdk.ICluster),
	}

	cm.create = newCluster
	return cm
}

func (cm *clusterMgr) ListCluster() []*sdk.ClusterInfo {
	arr := make([]*sdk.ClusterInfo, 0, len(cm.clusterMap))
	cm.clk.RLock()
	defer cm.clk.RUnlock()

	for _, v := range cm.clusterMap {
		arr = append(arr, v.Info())
	}

	return arr
}

func (cm *clusterMgr) getCluster(cId string) sdk.ICluster {
	cm.clk.RLock()
	defer cm.clk.RUnlock()

	return cm.clusterMap[cId]
}

func (cm *clusterMgr) putCluster(cId string, newC sdk.ICluster) {
	cm.clk.Lock()
	defer cm.clk.Unlock()

	cm.clusterMap[cId] = newC
}

func (cm *clusterMgr) AddCluster(ctx context.Context, cId string, masterAddr string) error {
	// check if cluster exist
	c := cm.getCluster(cId)
	if c != nil {
		if c.Addr() == masterAddr {
			return nil
		}
		// update masterAddr
		return c.UpdateAddr(ctx, masterAddr)
	}

	c, err := cm.create(ctx, cId, masterAddr)
	if err != nil {
		return err
	}

	cm.putCluster(cId, c)
	return nil
}

func (cm *clusterMgr) GetCluster(cId string) sdk.ICluster {
	return cm.getCluster(cId)
}
