package sdk

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type ICluster interface {
	GetVol(name string) IVolume
	//ListVols caller should cache result
	ListVols() []*VolInfo
	Info() *ClusterInfo
	addr() string
	updateAddr(ctx context.Context, addr string) error
}

// test can reset newMaster func
var newMaster = newSdkMasterCli

type cluster struct {
	masterAddr string
	clusterId  string

	cli IMaster

	volLk  sync.RWMutex
	volMap map[string]IVolume
	newVol func(ctx context.Context, cli IMaster, name, owner string) (IVolume, error)
}

func newCluster(ctx context.Context, addr, cId string) (ICluster, error) {
	cl := &cluster{
		masterAddr: addr,
		clusterId:  cId,
	}

	cl.volMap = make(map[string]IVolume)

	cli, err := initMasterCli(ctx, cId, addr)
	if err != nil {
		return nil, err
	}

	cl.cli = cli
	// get all volume in cluster
	err = cl.updateVols(ctx)
	if err != nil {
		return nil, err
	}

	go func() {
		cl.scheduleUpdateVols()
	}()

	return cl, nil
}

type ClusterInfo struct {
	Cid string
	// extend
}

func (c *cluster) Info() *ClusterInfo {
	ci := &ClusterInfo{
		Cid: c.clusterId,
	}

	return ci
}

func (c *cluster) GetVol(name string) IVolume {
	c.volLk.RLock()
	defer c.volLk.RUnlock()

	return c.volMap[name]
}

func (c *cluster) ListVols() []*VolInfo {
	c.volLk.RLock()
	defer c.volLk.RUnlock()

	infos := make([]*VolInfo, 0, len(c.volMap))
	for _, v := range c.volMap {
		infos = append(infos, v.Info())
	}

	return infos
}

func (c *cluster) putVol(name string, vol IVolume) {
	c.volLk.Lock()
	defer c.volLk.Unlock()

	c.volMap[name] = vol
}

func (c *cluster) addr() string {
	return c.masterAddr
}

//updateAddr need check whether newAddr is valid
func (c *cluster) updateAddr(ctx context.Context, addr string) error {
	_, err := initMasterCli(ctx, c.clusterId, addr)
	if err != nil {
		return err
	}
	fmt.Sprintf("update clster's addr, cId %s, addr %s, before%s", c.clusterId, addr, c.masterAddr)
	c.masterAddr = addr
	return nil
}

func initMasterCli(ctx context.Context, cId, addr string) (IMaster, error) {
	cli := newMaster(addr)
	info, err := cli.GetClusterIP()
	if err != nil {
		fmt.Sprintf("use Master Addr request failed, addr %s, err %s", addr, err.Error())
		return nil, err
	}

	if cId != info.Cluster {
		fmt.Sprintf("clusterId is not valid, local %s, right %s", cId, info.Cluster)
		return nil, ErrBadRequest
	}

	return cli, nil
}

func (c *cluster) scheduleUpdateVols() {
	ticker := time.NewTicker(time.Minute)
	for range ticker.C {
		ctx := context.Background()
		c.updateVols(ctx)
	}
}

func (c *cluster) updateVols(ctx context.Context) error {
	var returnErr error
	// keywords used to get target volumes
	vols, err := c.cli.ListVols("")
	if err != nil {
		fmt.Errorf("get volume list failed, err %s", err.Error())
		return masterToSdkErr(err)
	}

	for _, vol := range vols {
		// vol is deleted
		if vol.Status == 1 {
			// todo: delete from map
			continue
		}

		// already exist
		old := c.GetVol(vol.Name)
		if old != nil {
			continue
		}

		newVol, err := c.newVol(ctx, c.cli, vol.Name, vol.Owner)
		if err != nil {
			fmt.Sprintf("new volume failed, name %s, err %s", vol.Name, vol.Owner)
			returnErr = err
			continue
		}

		c.putVol(vol.Name, newVol)
	}

	return returnErr
}
