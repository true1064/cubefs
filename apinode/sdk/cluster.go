package sdk

import "sync"

type ICluster interface {
	GetVol(name string) Volume
	addr() string
	updateAddr(addr string) error
}

type cluster struct {
	masterAddr string
	clusterId  string

	volLk  sync.RWMutex
	volMap map[string]Volume
}

func newCluster(addr, clusterId string) (ICluster, error) {
	cl := &cluster{
		masterAddr: addr,
		clusterId:  clusterId,
	}

	cl.volMap = make(map[string]Volume)
	return cl, nil
}

func (c *cluster) GetVol(name string) Volume {
	return nil
}

func (c *cluster) addr() string {
	return c.masterAddr
}

//updateAddr need check whether newAddr is valid
func (c *cluster) updateAddr(addr string) error {
	c.masterAddr = addr
	return nil
}
