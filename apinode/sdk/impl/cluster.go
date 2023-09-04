package impl

import (
	"context"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/pkg/errors"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// test can reset newMaster func
var newMaster = newSdkMasterCli

type cluster struct {
	masterAddr string
	clusterId  string
	lock       sync.RWMutex
	fileId     *proto.FileId

	cli IMaster

	volLk  sync.RWMutex
	volMap map[string]sdk.IVolume
	newVol func(ctx context.Context, name, owner, addr string) (sdk.IVolume, error)
}

func newClusterIn(ctx context.Context, addr, cId string) (*cluster, error) {
	cl := &cluster{
		masterAddr: addr,
		clusterId:  cId,
	}

	cl.volMap = make(map[string]sdk.IVolume)

	cli, err := initMasterCli(ctx, cId, addr)
	if err != nil {
		return nil, err
	}

	cl.newVol = newVolume
	cl.cli = cli
	cl.fileId, err = cli.AllocFileId()
	if err != nil {
		return nil, errors.Wrap(err, "alloc file id failed")
	}
	return cl, nil
}

func newCluster(ctx context.Context, cId, addr string) (sdk.ICluster, error) {
	cl, err := newClusterIn(ctx, addr, cId)
	if err != nil {
		return nil, err
	}
	// get all volume in cluster
	cl.updateVols(ctx)
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

func (c *cluster) allocFileId(ctx context.Context) (id uint64, err error) {
	span := trace.SpanFromContextSafe(ctx)

	c.lock.Lock()
	defer c.lock.Unlock()

	if c.fileId.Begin < c.fileId.End {
		c.fileId.Begin++
		return c.fileId.Begin, nil
	}

	start := time.Now()
	fileId, err := c.cli.AllocFileId()
	if err != nil {
		span.Errorf("alloc file id failed, err %s", err.Error())
		return 0, err
	}
	span.Infof("alloc fileId success, id %v, cost %s", fileId, time.Since(start).String())

	c.fileId = fileId
	c.fileId.Begin++
	return c.fileId.Begin, nil
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
	span := trace.SpanFromContextSafe(ctx)
	span.Warnf("update cluster's addr success, cId %s, addr %s, before %s", c.clusterId, addr, c.masterAddr)
	c.masterAddr = addr
	return nil
}

func initMasterCli(ctx context.Context, cId, addr string) (IMaster, error) {
	span := trace.SpanFromContextSafe(ctx)
	cli := newMaster(addr)
	info, err := cli.GetClusterIP()
	if err != nil {
		span.Errorf("user master addr request failed, addr %s, err %s", addr, err.Error())
		return nil, masterToSdkErr(err)
	}

	if cId != info.Cluster {
		span.Errorf("clusterId is not valid, local %s, right %s, addr %s", cId, info.Cluster, addr)
		return nil, sdk.ErrBadRequest
	}

	return cli, nil
}

func (c *cluster) scheduleUpdateVols() {
	ticker := time.NewTicker(time.Minute)
	for range ticker.C {
		span, ctx := trace.StartSpanFromContext(context.TODO(), "")
		err := c.updateVols(ctx)
		if err != nil {
			span.Errorf("update vol failed", err.Error())
		}
	}
}

func (c *cluster) updateVols(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	// keywords used to get target volumes
	vols, err := c.cli.ListVols("")
	if err != nil {
		span.Errorf("get volume list failed, err %s", err.Error())
		return masterToSdkErr(err)
	}

	var returnErr error
	for _, vol := range vols {
		old := c.GetVol(vol.Name)
		if old != nil {
			continue
		}

		newVol, err := c.newVol(ctx, vol.Name, vol.Owner, c.masterAddr)
		if err != nil {
			span.Errorf("new volume failed, name %s, err %s", vol.Name, err.Error())
			returnErr = err
			continue
		}

		if v, ok := newVol.(*volume); ok {
			v.allocId = c.allocFileId
		}

		c.putVol(vol.Name, newVol)
	}

	return returnErr
}
