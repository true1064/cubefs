package drive

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/cubefs/cubefs/apinode/oplog"
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type logItem struct {
	Op        OpCode `json:"op"`
	Path      string `json:"path"`
	Uid       string `json:"uid"`
	Reqid     string `json:"reqid"`
	Timestamp int64  `json:"timestamp"`
}

func (d *DriveNode) ConsumerEvent(ctx context.Context, e oplog.Event) {
	data, err := json.Marshal(e.Fields)
	if err != nil {
		return
	}

	item := logItem{}
	if err = json.Unmarshal(data, &item); err != nil {
		return
	}

	if item.Op != OpMultiUploadFile && item.Op != OpUpdateFile {
		return
	}

	// if return false, should retry, return true, don't retry
	calculate := func() bool {
		span, sctx := trace.StartSpanFromContextWithTraceID(context.Background(), "consumer", item.Reqid)
		ur, err := d.GetUserRouteInfo(sctx, UserID(item.Uid))
		if err != nil {
			span.Warnf("get user route info error: %v, %v", err, item)
			return err == sdk.ErrNotFound
		}
		cluster := d.clusterMgr.GetCluster(ur.ClusterID)
		if cluster == nil {
			span.Warnf("not found cluster %v", item)
			return true
		}
		volume := cluster.GetVol(ur.VolumeID)
		if volume == nil {
			span.Warnf("not found volume %v", item)
			return true
		}
		dInfo, err := d.lookup(sctx, volume, ur.RootFileID, item.Path)
		if err != nil {
			span.Warnf("lookup error: %v, %v", err, item)
			return err == sdk.ErrNotFound
		}
		if dInfo.IsDir() {
			span.Warnf("%v is not file", item)
			return true
		}
		inode, err := volume.GetInode(sctx, dInfo.Inode)
		if err != nil {
			span.Warnf("get inode error: %v, %v", err, item)
			return err == sdk.ErrNotFound
		}

		r, err := d.makeBlockedReader(sctx, volume, dInfo.Inode, 0, ur.CipherKey)
		if err != nil {
			span.Warnf("make blocked reader error: %v, %v", err, item)
			return false
		}

		wr := md5.New()
		te := io.TeeReader(newFixedReader(r, int64(inode.Size)), wr)
		if _, err = io.Copy(io.Discard, te); err != nil {
			span.Warnf("copy error: %v, %v", err, item)
			return false
		}

		md5sum := fmt.Sprintf("%x", wr.Sum(nil))
		if err = volume.SetXAttr(sctx, dInfo.Inode, "x-cfa-md5", md5sum); err != nil {
			span.Warnf("set x-cfa-md5  error: %v, %v", err, item)
			return false
		}
		return true
	}

	for i := 0; i < 3; i++ {
		if calculate() {
			return
		}
		select {
		case <-time.After(10 * time.Second):
		case <-ctx.Done():
			return
		}
	}
	data = append(data, '\n')
	if _, err := d.recorder.Write(data); err != nil {
		log.Panicf("write recorder file error: %v", err)
	}
	processOplogFailedCount.Add(1)
}
