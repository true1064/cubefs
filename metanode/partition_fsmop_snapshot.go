package metanode

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"time"
)

func (mp *metaPartition) fsmCreateDirSnapshot(ifo *proto.DirSnapShotInfo) (resp uint8) {
	oldItem := mp.dirVerTree.CopyGet(newDirSnapItem(ifo.SnapshotInode))
	if oldItem == nil {
		oldItem = &DirSnapshotItem{
			SnapshotInode: ifo.SnapshotInode,
			Dir:           ifo.SnapshotDir,
			RootInode:     ifo.RootInode,
			Vers:          []*SnapshotVer{},
		}
		mp.dirVerTree.ReplaceOrInsert(oldItem, true)
	}

	oldDirSnap := oldItem.(*DirSnapshotItem)
	if ifo.RootInode != oldDirSnap.RootInode || ifo.SnapshotDir != oldDirSnap.Dir {
		log.LogWarnf("fsmCreateDirSnapshot: root inode is conflict with before, req %v, before %v", ifo, oldItem)
		return proto.OpArgMismatchErr
	}

	oldDirSnap.Lock()
	defer oldDirSnap.Unlock()

	// check if conflict witch before
	for _, v := range oldDirSnap.Vers {
		if ifo.Ver == v.Ver && ifo.OutVer == v.OutVer {
			return proto.OpOk
		}
		if ifo.Ver == v.Ver || ifo.OutVer == v.OutVer {
			log.LogWarnf("fsmCreateDirSnapshot: req is conflict with before, ifo %v, v %v", ifo, v)
			return proto.OpArgMismatchErr
		}
	}

	oldDirSnap.Vers = append(oldDirSnap.Vers, newSnapshotVer(ifo.OutVer, ifo.Ver))
	if log.EnableInfo() {
		log.LogInfof("create dir snapshot success, ifo %v, oldDirSnap %s", ifo, oldDirSnap.String())
	}
	return proto.OpOk
}

// mark delete dir snapshot
func (mp *metaPartition) fsmDelDirSnap(e *proto.DirVerItem) (resp uint8) {
	if log.EnableDebug() {
		log.LogDebugf("fsmDelDirSnap: start delete dir snapshot, e%v", e)
	}

	item := mp.dirVerTree.CopyGet(newDirSnapItem(e.DirSnapIno))
	if item == nil {
		log.LogWarnf("fsmDelDirSnap: target dir snapshot is not exist, req %v", e)
		return proto.OpNotExistErr
	}

	oldDirItem := item.(*DirSnapshotItem)
	for _, v := range oldDirItem.Vers {
		if v.Ver == e.Ver {
			if log.EnableDebug() {
				log.LogDebugf("fsmDelDirSnap: delete dir snapshot success, req %v, dir %s, ver %v",
					e, oldDirItem.String(), v)
			}
			v.Status = proto.VersionMarkDelete
			return proto.OpOk
		}
	}

	log.LogWarnf("fsmDelDirSnap: target dir snapshot ver is not exist, req %v, ver %v", e, oldDirItem.Vers)
	return proto.OpNotExistErr
}

func (mp *metaPartition) fsmBatchDelDirSnapshot(ifo *BatchDelDirSnapInfo) (resp uint8) {
	start := time.Now()
	log.LogDebugf("fsmBatchDelDirSnapshot: start batch delete, stat %d, cnt %d", ifo.Status, len(ifo.Items))
	if ifo.Status != proto.VersionDeleting && ifo.Status != proto.VersionDeleted {
		log.LogWarnf("fsmBatchDelDirSnapshot: ifo status is not valid, status %d", ifo.Status)
		return proto.OpArgMismatchErr
	}

	resp = proto.OpOk
	verMap := map[string]struct{}{}
	inoMap := map[uint64]struct{}{}

	getKey := func(ino, ver uint64) string {
		return fmt.Sprintf("%d_%d", ino, ver)
	}

	for _, e := range ifo.Items {
		key := getKey(e.DirSnapIno, e.Ver)
		verMap[key] = struct{}{}
		inoMap[e.DirSnapIno] = struct{}{}
	}

	delInoSlice := make([]*DirSnapshotItem, 0)

	delVer := func(dir *DirSnapshotItem) bool {
		verItems := make([]*SnapshotVer, 0, len(dir.Vers))
		for _, v := range dir.Vers {
			key := getKey(dir.SnapshotInode, v.Ver)
			if _, ok := verMap[key]; !ok {
				verItems = append(verItems, v)
				continue
			}

			if v.Status != proto.VersionDeleting {
				log.LogErrorf("fsmBatchDelDirSnapshot: snapshot version is not illegal, ino %d, ver %v", dir.SnapshotInode, v)
				resp = proto.OpInternalErr
				return false
			}

			if log.EnableDebug() {
				log.LogDebugf("fsmBatchDelDirSnapshot: delete dir snapshot, dir %s, ino %d, ver %v", dir.Dir, dir.SnapshotInode, v)
			}
		}

		if len(verItems) == 0 {
			delInoSlice = append(delInoSlice, dir)
			return true
		}

		dir.Vers = verItems
		return true
	}

	updateVer := func(dir *DirSnapshotItem) bool {
		for _, v := range dir.Vers {
			key := getKey(dir.SnapshotInode, v.Ver)
			if _, ok := verMap[key]; !ok {
				continue
			}

			if v.Status != proto.VersionMarkDelete && v.Status != proto.VersionDeleting {
				log.LogErrorf("fsmBatchDelDirSnapshot: snapshot version is not illegal, ino %d, ver %v",
					dir.SnapshotInode, v)
				resp = proto.OpInternalErr
				return false
			}

			v.Status = proto.VersionDeleting
			if log.EnableDebug() {
				log.LogDebugf("fsmBatchDelDirSnapshot: dir snapshot deleting, dir %s, ino %d, ver %v",
					dir.Dir, dir.SnapshotInode, v)
			}
		}
		return true
	}

	mp.dirVerTree.Ascend(func(i BtreeItem) bool {
		dir := i.(*DirSnapshotItem)

		if _, ok := inoMap[dir.SnapshotInode]; !ok {
			return true
		}

		dir.Lock()
		defer dir.Unlock()

		if ifo.Status == proto.VersionDeleting {
			return updateVer(dir)
		}

		return delVer(dir)
	})

	if resp != proto.OpOk {
		return resp
	}

	for _, ino := range delInoSlice {
		mp.dirVerTree.Delete(ino)
		log.LogDebugf("fsmBatchDelDirSnapshot: delete dir snapshot, dir ino(%d), dir %s",
			ino.SnapshotInode, ino.Dir)
	}

	log.LogDebugf("fsmBatchDelDirSnapshot: delete snapshot end, stat %d, cnt %d, cost %s",
		ifo.Status, len(ifo.Items), time.Since(start).String())
	return proto.OpOk
}

func (mp *metaPartition) getDirSnapItem(dirIno uint64) *DirSnapshotItem {
	oldItem := mp.dirVerTree.Get(newDirSnapItem(dirIno))
	if oldItem != nil {
		return oldItem.(*DirSnapshotItem)
	}
	return nil
}
