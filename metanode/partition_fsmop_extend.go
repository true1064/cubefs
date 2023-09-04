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
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"strconv"
	"strings"
	"time"
)

type ExtendOpResult struct {
	Status uint8
	Extend *Extend
}

func (mp *metaPartition) fsmSetXAttr(extend *Extend) (err error) {
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
			e.multiVers = append(e.multiVers, e.Copy().(*Extend))
			e.verSeq = extend.verSeq
		}
		e.Merge(extend, true)
	}

	return
}

func (mp *metaPartition) fsmSetXAttrEx(extend *Extend) (resp uint8, err error) {
	log.LogDebugf("start execute fsmSetXAttrEx, ino %d", extend.inode)
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
		if oldV, exist := e.Get([]byte(key)); exist {
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

// TODO support snap
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

// todo(leon chang):check snapshot delete relation with attr
func (mp *metaPartition) fsmRemoveXAttr(extend *Extend) (err error) {
	treeItem := mp.extendTree.CopyGet(extend)
	if treeItem == nil {
		return
	}
	e := treeItem.(*Extend)
	if extend.verSeq < e.verSeq {
		return fmt.Errorf("seq error assign %v but less than %v", extend.verSeq, e.verSeq)
	}
	// attr multi-ver copy all attr for simplify management
	if e.verSeq > extend.verSeq {
		e.multiVers = append(e.multiVers, e)
		e.verSeq = extend.verSeq
	}

	extend.Range(func(key, value []byte) bool {
		e.Remove(key)
		return true
	})
	return
}
