// Copyright 2023 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package metanode

import (
	"encoding/json"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (mp *metaPartition) ListAllDirSnapshot(rootIno uint64, p *Packet) (err error) {
	log.LogDebugf("ListAllDirSnapshot: start ListAllDirSnapshot, ino %d", rootIno)

	startItem := newDirSnapItem(0, rootIno)
	endItem := newDirSnapItem(0, rootIno+1)
	resp := &proto.ListDirSnapshotResp{}

	mp.dirVerTree.AscendRange(startItem, endItem, func(i BtreeItem) bool {
		dirVer := i.(*dirSnapshotItem)
		resp.Items = append(resp.Items, dirVer.buildDirSnapshotIfo())
		return true
	})

	reply, err := json.Marshal(resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(reply)

	log.LogDebugf("ListAllDirSnapshot: list dir snapshot success, rootIno %d, cnt %d", rootIno, len(resp.Items))
	return
}

func (mp *metaPartition) DelDirSnapshot(ifo *proto.DirVerItem, p *Packet) (err error) {
	log.LogDebugf("DelDirSnapshot: start delete dir snapshot, ifo %v", ifo)

	val, err := json.Marshal(ifo)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}

	resp, err := mp.submit(opFSMMarkDelDirSnap, val)
	if err != nil {
		log.LogErrorf("DelDirSnapshot: submit create dir snapshot raft cmd failed, ifo %v, err %s", ifo, err.Error())
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	p.ResultCode = resp.(uint8)
	log.LogDebugf("DelDirSnapshot: delete dir snapshot success: ifo %v, status %d", ifo, p.ResultCode)
	return
}

func (mp *metaPartition) BatchDelDirSnapshot(items []proto.DirVerItem, p *Packet) (err error) {
	if log.EnableDebug() {
		for _, e := range items {
			log.LogDebugf("DelDirSnapshot: start delete dir snapshot, ifo %v", e)
		}
	}

	info := &BatchDelDirSnapInfo{}
	info.Status = proto.VersionDeleted
	val, err := json.Marshal(info)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}

	resp, err := mp.submit(opFSMBatchDelDirSnap, val)
	if err != nil {
		log.LogErrorf("DelDirSnapshot: submit create dir snapshot raft cmd failed, ifo %v, err %s", info, err.Error())
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	p.ResultCode = resp.(uint8)
	log.LogDebugf("DelDirSnapshot: delete dir snapshot success: ifo %v, status %d", info, p.ResultCode)
	return
}

func (mp *metaPartition) CreateDirSnapshot(ifo *proto.CreateDirSnapShotInfo, p *Packet) (err error) {
	log.LogDebugf("CreateDirSnapshot: start create dir snapshot, ifo %v", ifo)

	val, err := json.Marshal(ifo)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}

	resp, err := mp.submit(opFSMCreateDirSnap, val)
	if err != nil {
		log.LogErrorf("CreateDirSnapshot: submit create dir snapshot raft cmd failed, ifo %v, err %s", ifo, err.Error())
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	p.ResultCode = resp.(uint8)
	log.LogDebugf("CreateDirSnapshot: create dir snapshot success: ifo %v, status %d", ifo, p.ResultCode)
	return
}
