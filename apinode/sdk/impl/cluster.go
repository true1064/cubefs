package impl

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/apinode/sdk"
)

// test can reset newMaster func
var newMaster = newSdkMasterCli

type cluster struct {
	masterAddr string
	clusterId  string

	cli sdk.IMaster

	volLk  sync.RWMutex
	volMap map[string]sdk.IVolume
	newVol func(ctx context.Context, cli sdk.IMaster, name, owner string) (sdk.IVolume, error)
}

func newCluster(ctx context.Context, addr, cId string) (sdk.ICluster, error) {
	cl := &cluster{
		masterAddr: addr,
		clusterId:  cId,
	}

	cl.volMap = make(map[string]sdk.IVolume)

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

func (c *cluster) Info() *sdk.ClusterInfo {
	ci := &sdk.ClusterInfo{
		Cid: c.clusterId,
	}

	return ci
}

func (c *cluster) GetVol(name string) sdk.IVolume {
	c.volLk.RLock()
	defer c.volLk.RUnlock()

	return c.volMap[name]
}

func (c *cluster) ListVols() []*sdk.VolInfo {
	c.volLk.RLock()
	defer c.volLk.RUnlock()

	infos := make([]*sdk.VolInfo, 0, len(c.volMap))
	for _, v := range c.volMap {
		infos = append(infos, v.Info())
	}

	return infos
}

func (c *cluster) putVol(name string, vol sdk.IVolume) {
	c.volLk.Lock()
	defer c.volLk.Unlock()

	c.volMap[name] = vol
}

func (c *cluster) Addr() string {
	return c.masterAddr
}

// updateAddr need check whether newAddr is valid
func (c *cluster) UpdateAddr(ctx context.Context, addr string) error {
	_, err := initMasterCli(ctx, c.clusterId, addr)
	if err != nil {
		return err
	}
	fmt.Printf("update clster's addr, cId %s, addr %s, before%s", c.clusterId, addr, c.masterAddr)
	c.masterAddr = addr
	return nil
}

func initMasterCli(ctx context.Context, cId, addr string) (sdk.IMaster, error) {
	cli := newMaster(addr)
	info, err := cli.GetClusterIP()
	if err != nil {
		fmt.Printf("use Master Addr request failed, addr %s, err %s", addr, err.Error())
		return nil, masterToSdkErr(err)
	}

	if cId != info.Cluster {
		fmt.Printf("clusterId is not valid, local %s, right %s", cId, info.Cluster)
		return nil, sdk.ErrBadRequest
	}

	return cli, nil
}

func (c *cluster) scheduleUpdateVols() {
	ticker := time.NewTicker(time.Minute)
	for range ticker.C {
		ctx := context.Background()
		err := c.updateVols(ctx)
		if err != nil {
			// todo log
		}
	}
}

func (c *cluster) updateVols(ctx context.Context) error {
	var returnErr error
	// keywords used to get target volumes
	vols, err := c.cli.ListVols("")
	if err != nil {
		fmt.Printf("get volume list failed, err %s", err.Error())
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
			fmt.Printf("new volume failed, name %s, err %s", vol.Name, vol.Owner)
			returnErr = err
			continue
		}

		c.putVol(vol.Name, newVol)
	}

	return returnErr
}
