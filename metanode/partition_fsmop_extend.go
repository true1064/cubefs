// Copyright 2018 The CubeFS Authors.
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
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

type ExtendOpResult struct {
	Status uint8
	Extend *Extend
}

func (mp *metaPartition) fsmSetXAttr(extend *Extend) (err error) {
	log.LogDebugf("start execute fsmSetXAttr, ino %d, seq %d", extend.inode, extend.verSeq)
	treeItem := mp.extendTree.CopyGet(extend)
	var e *Extend
	if treeItem == nil {
		mp.extendTree.ReplaceOrInsert(extend, true)
	} else {
		// attr multi-ver copy all attr for simplify management
		e = treeItem.(*Extend)
		if e.verSeq != extend.verSeq {
			if extend.verSeq < e.verSeq {
				return fmt.Errorf("seq error assign %v but less than %v", extend.verSeq, e.verSeq)
			}
			e.multiVers = append([]*Extend{e.Copy().(*Extend)}, e.multiVers...)
			e.verSeq = extend.verSeq
		}
		e.Merge(extend, true)
	}

	return
}

func (mp *metaPartition) fsmSetXAttrEx(extend *Extend) (resp uint8, err error) {
	log.LogDebugf("start execute fsmSetXAttrEx, ino %d, seq %d", extend.inode, extend.verSeq)
	resp = proto.OpOk
	treeItem := mp.extendTree.Get(extend)
	var e *Extend
	if treeItem == nil {
		log.LogDebugf("fsmSetXAttrEx not exist, set it directly, ino %d", extend.inode)
		err = mp.fsmSetXAttr(extend)
		return
	}
	// attr multi-ver copy all attr for simplify management
	e = treeItem.(*Extend)
	for key, v := range extend.dataMap {
		if oldV, exist := e.Get([]byte(key)); exist && e.verSeq == extend.verSeq {
			log.LogWarnf("fsmSetXAttrEx: target key is already exist, ino %d, key %s, val %s, old %s",
				extend.inode, key, string(v), string(oldV))
			resp = proto.OpExistErr
			return
		}
	}

	err = mp.fsmSetXAttr(extend)
	log.LogDebugf("fsmSetXAttrEx, set xAttr success, ino %d", extend.inode)
	return
}

// TODO support snap, not use
func (mp *metaPartition) fsmSetInodeLock(req *proto.InodeLockReq) (status uint8) {
	if req.LockType == proto.InodeLockStatus {
		return mp.inodeLock(req)
	} else if req.LockType == proto.InodeUnLockStatus {
		return mp.inodeUnlock(req)
	} else {
		log.LogErrorf("request type is not valid, type %d", req.LockType)
		return proto.OpArgMismatchErr
	}
}

func (mp *metaPartition) inodeLock(req *proto.InodeLockReq) (status uint8) {
	tmpE := &Extend{inode: req.Inode}
	now := time.Now().Unix()
	val := fmt.Sprintf("%s-%d", req.Id, now+int64(req.ExpireTime))
	key := []byte(proto.InodeLockKey)

	var e *Extend
	item := mp.extendTree.CopyGet(tmpE)
	if item == nil {
		e = NewExtend(req.Inode)
		e.Put([]byte(proto.InodeLockKey), []byte(val), 0)
		mp.extendTree.ReplaceOrInsert(e, true)
		return proto.OpOk
	}
	e = item.(*Extend)

	oldVal, exist := e.Get(key)
	if !exist {
		e.Put(key, []byte(val), 0)
		return proto.OpOk
	}

	oldValStr := strings.TrimSpace(string(oldVal))
	if len(oldVal) == 0 {
		e.Put(key, []byte(val), 0)
		return proto.OpOk
	}

	arr := strings.Split(oldValStr, "-")
	if len(arr) != 2 {
		log.LogErrorf("inode val is not valid, ino %d, val %s", req.Inode, oldValStr)
		return proto.OpArgMismatchErr
	}

	expireTime, err := strconv.Atoi(arr[1])
	if err != nil {
		log.LogErrorf("inode val is not valid, ino %d, val %s", req.Inode, oldValStr)
		return proto.OpArgMismatchErr
	}

	if now > int64(expireTime) {
		e.Put(key, []byte(val), 0)
		return proto.OpOk
	}

	return proto.OpExistErr
}

func (mp *metaPartition) inodeUnlock(req *proto.InodeLockReq) (status uint8) {
	tmpE := &Extend{inode: req.Inode}
	key := []byte(proto.InodeLockKey)
	item := mp.extendTree.CopyGet(tmpE)

	if item == nil {
		log.LogErrorf("inode lock, extend still not exist, inode %d", req.Inode)
		return proto.OpArgMismatchErr
	}

	var e *Extend
	e = item.(*Extend)
	oldVal, ok := e.Get(key)
	if !ok {
		log.LogErrorf("inode unlock, lock key not exist, inode %d", req.Inode)
		return proto.OpArgMismatchErr
	}

	arr := strings.Split(string(oldVal), "-")
	if arr[0] == req.Id {
		e.Remove(key)
		return proto.OpOk
	}

	log.LogErrorf("inodeUnlock lock used by other, id %s, val %s", req.Id, string(oldVal))
	return proto.OpArgMismatchErr
}

// func (mp *metaPartition) clearInodeDirXAttr(reqExtend *Extend, vers []*proto.VersionInfo) (err error){
// 	if log.EnableDebug() {
// 		log.LogDebugf("start delete dir xattr, req %v, vers %v", reqExtend, vers)
// 	}

// 	treeItem := mp.extendTree.CopyGet(reqExtend)
// 	if treeItem == nil {
// 		return
// 	}

// 	verList := mp.multiVersionList
// 	curVer := mp.GetVerSeq()
// 	if vers != nil {
// 		verList = &proto.VolVersionInfoList{
// 			VerList: vers,
// 		}
// 		curVer = verList.GetLastVer()
// 	}

// 	e := treeItem.(*Extend)
// 	// remove from newest version
// 	if curVer == 0 || (e.verSeq == curVer && reqExtend.verSeq == 0) {
// 		reqExtend.Range(func(key, value []byte) bool {
// 			e.Remove(key)
// 			return true
// 		})
// 		return
// 	}

// 	if reqExtend.verSeq == 0 {
// 		reqExtend.verSeq = curVer
// 	}
// 	if reqExtend.verSeq == math.MaxUint64 {
// 		reqExtend.verSeq = 0
// 	}

// 	e.versionMu.Lock()
// 	defer e.versionMu.Unlock()

// 	// less than minum ver
// 	if reqExtend.verSeq < e.GetMinVer() {
// 		return
// 	}

// 	// delete ver large than extend ver
// 	if reqExtend.verSeq > e.verSeq {
// 		e.multiVers = append([]*Extend{e.Copy().(*Extend)}, e.multiVers...)
// 		e.verSeq = reqExtend.verSeq
// 		reqExtend.Range(func(key, value []byte) bool {
// 			e.Remove(key)
// 			return true
// 		})
// 		return
// 	}

// 	// delete ver equal to extend ver
// 	if reqExtend.verSeq == e.verSeq {
// 		var globalNewVer uint64
// 		if globalNewVer, err = verList.GetNextNewerVer(reqExtend.verSeq); err != nil {
// 			log.LogErrorf("fsmRemoveXAttr. mp [%v] seq %v req ver %v not found newer seq", mp.config.PartitionId, mp.verSeq, reqExtend.verSeq)
// 			return err
// 		}
// 		e.verSeq = globalNewVer
// 		return
// 	}

// 	// delete ver in version list
// 	innerLastVer := e.verSeq
// 	for id, ele := range e.multiVers {
// 		if ele.verSeq > reqExtend.verSeq {
// 			innerLastVer = ele.verSeq
// 			continue
// 		} else if ele.verSeq < reqExtend.verSeq {
// 			return
// 		} else {
// 			var globalNewVer uint64
// 			if globalNewVer, err = verList.GetNextNewerVer(ele.verSeq); err != nil {
// 				return err
// 			}
// 			if globalNewVer < innerLastVer {
// 				log.LogDebugf("mp %v inode %v extent layer %v update seq %v to %v",
// 					mp.config.PartitionId, ele.inode, id, ele.verSeq, globalNewVer)
// 				ele.verSeq = globalNewVer
// 				return
// 			}
// 			e.multiVers = append(e.multiVers[:id], e.multiVers[id+1:]...)
// 			return
// 		}
// 	}

// 	return
// }

func (mp *metaPartition) fsmRemoveDirXAttr(reqExtend *Extend, vers []*proto.VersionInfo) (err error) {
	treeItem := mp.extendTree.CopyGet(reqExtend)
	if treeItem == nil {
		return
	}

	verList := mp.multiVersionList
	curVer := mp.GetVerSeq()
	if vers != nil {
		verList = &proto.VolVersionInfoList{
			VerList: vers,
		}
		curVer = verList.GetLastVer()
	}

	e := treeItem.(*Extend)
	if log.EnableDebug() {
		log.LogDebugf("fsmRemoveDirXAttr: start delete dir xattr, req %v, vers %v, oldVer %v", reqExtend, vers, e.verSeq)
	}

	// remove from newest version
	if curVer == 0 || (e.verSeq == curVer && reqExtend.verSeq == 0) {
		reqExtend.Range(func(key, value []byte) bool {
			e.Remove(key)
			return true
		})
		return
	}

	if reqExtend.verSeq == 0 {
		reqExtend.verSeq = curVer
	}
	if reqExtend.verSeq == math.MaxUint64 {
		reqExtend.verSeq = 0
	}

	e.versionMu.Lock()
	defer e.versionMu.Unlock()

	// less than minum ver
	if reqExtend.verSeq < e.GetMinVer() {
		return
	}

	// delete ver large than extend ver
	if reqExtend.verSeq > e.verSeq {
		e.multiVers = append([]*Extend{e.Copy().(*Extend)}, e.multiVers...)
		e.verSeq = reqExtend.verSeq
		reqExtend.Range(func(key, value []byte) bool {
			e.Remove(key)
			return true
		})
		log.LogDebugf("fsmRemoveDirXAttr: delete xattr success, delSeq %d, e.verLen(%d), ino %d",
			reqExtend.verSeq, len(e.multiVers), e.inode)
		return
	}

	// delete ver equal to extend ver
	if reqExtend.verSeq == e.verSeq {
		var globalNewVer uint64
		if globalNewVer, err = verList.GetNextNewerVer(reqExtend.verSeq); err != nil {
			log.LogErrorf("fsmRemoveDirXAttr. mp [%v] seq %v req ver %v not found newer seq", mp.config.PartitionId, mp.verSeq, reqExtend.verSeq)
			return err
		}
		e.verSeq = globalNewVer
		return
	}

	// delete ver in version list
	innerLastVer := e.verSeq
	for id, ele := range e.multiVers {
		if ele.verSeq > reqExtend.verSeq {
			innerLastVer = ele.verSeq
			continue
		} else if ele.verSeq < reqExtend.verSeq {
			return
		} else {
			var globalNewVer uint64
			if globalNewVer, err = verList.GetNextNewerVer(ele.verSeq); err != nil {
				return err
			}
			if globalNewVer < innerLastVer {
				log.LogDebugf("fsmRemoveDirXAttr：mp %v inode %v extent layer %v update seq %v to %v",
					mp.config.PartitionId, ele.inode, id, ele.verSeq, globalNewVer)
				ele.verSeq = globalNewVer
				return
			}
			e.multiVers = append(e.multiVers[:id], e.multiVers[id+1:]...)
			return
		}
	}

	return
}

// todo(leon chang):check snapshot delete relation with attr
func (mp *metaPartition) fsmRemoveXAttr(reqExtend *Extend) (err error) {
	return mp.fsmRemoveDirXAttr(reqExtend, nil)
}
