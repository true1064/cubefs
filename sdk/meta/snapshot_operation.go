package meta

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

func (mw *SnapShotMetaWrapper) listAllDirSnapshot(mp *MetaPartition, subRootIno uint64) (items []*proto.DirSnapshotInfo, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("listAllDirSnapshot", err, bgTime, 1)
	}()

	req := &proto.ListDirSnapshotReq{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		RootIno:     subRootIno,
	}
	resp := new(proto.ListDirSnapshotResp)
	err = mw.sendToMeta(mp, proto.OpMetaListDirVer, req, resp)
	if err != nil {
		return nil, err
	}
	return resp.Items, nil
}

func (mw *SnapShotMetaWrapper) createDirSnapshot(mp *MetaPartition, info *proto.CreateDirSnapShotInfo) (err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("createDirSnapshot", err, bgTime, 1)
	}()

	req := &proto.CreateDirSnapShotReq{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Info:        info,
	}

	return mw.sendToMeta(mp, proto.OpMetaCreateDirVer, req, nil)
}

func (mw *SnapShotMetaWrapper) delDirSnapshot(mp *MetaPartition, info *proto.DirVerItem) (err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("delDirSnapshot", err, bgTime, 1)
	}()

	req := &proto.DirVerDelReq{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Item:        info,
	}

	return mw.sendToMeta(mp, proto.OpMetaDelDirVer, req, nil)
}

func (mw *SnapShotMetaWrapper) sendToMeta(mp *MetaPartition, opCode uint8, req, resp interface{}) (err error) {
	pkt := proto.NewPacketReqID()
	pkt.Opcode = opCode
	pkt.PartitionID = mp.PartitionID
	err = pkt.MarshalData(req)
	if err != nil {
		log.LogErrorf("%s marshal failed: req(%v) err(%v)", pkt.GetOpMsg(), req, err)
		return
	}

	if log.EnableDebug() {
		log.LogDebugf("%s: pkt(%v) mp(%v) req(%v)", pkt.GetOpMsg(), pkt, mp, req)
	}

	metric := exporter.NewTPCnt(pkt.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	resultPkt, err := mw.sendToMetaPartition(mp, pkt)
	if err != nil {
		log.LogErrorf("%s: pkt(%v) mp(%v) req(%v) err(%v)", pkt.GetOpMsg(), pkt, mp, req, err)
		return
	}

	status := parseStatus(resultPkt.ResultCode)
	if status != statusOK {
		err = statusToErrno(status)
		log.LogErrorf("%s: pkt(%v) mp(%v) req(%v) result(%v), status (%v)",
			pkt.GetOpMsg(), pkt, mp, req, pkt.GetResultMsg(), status)
		return
	}

	if resp != nil {
		err = resultPkt.UnmarshalData(resp)
		if err != nil {
			log.LogErrorf("%s: packet(%v) mp(%v) req(%v) err(%v) PacketData(%v)",
				pkt.GetOpMsg(), resultPkt, mp, req, err, string(resultPkt.Data))
			return
		}
	}

	if log.EnableDebug() {
		log.LogDebugf("%s: pkt(%v) mp(%v) req(%v) result(%v)",
			pkt.GetOpMsg(), resultPkt, mp, req, resultPkt.GetResultMsg())
	}
	return
}
