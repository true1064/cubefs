package meta

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"syscall"
)

func (mw *SnapShotMetaWrapper) ListAllDirSnapshot(rootIno uint64) (items []*proto.DirSnapshotInfo, err error) {
	mp := mw.getPartitionByInode(rootIno)
	if mp == nil {
		log.LogErrorf("ListAllDirSnapshot: No such partition, ino(%v)", rootIno)
		return nil, syscall.EINVAL
	}

	items, err = mw.listAllDirSnapshot(mp, rootIno)
	if err != nil {
		log.LogErrorf("ListAllDirSnapshot: ino(%v) err(%v)", rootIno, err)
		return nil, err
	}

	return items, nil
}

func (mw *SnapShotMetaWrapper) CreateDirSnapshot(ifo *proto.CreateDirSnapShotInfo) (err error) {
	mp := mw.getPartitionByInode(ifo.RootInode)
	if mp == nil {
		log.LogErrorf("CreateDirSnapshot: No such partition, ino(%v)", ifo)
		return syscall.EINVAL
	}

	err = mw.createDirSnapshot(mp, ifo)
	if err != nil {
		log.LogErrorf("CreateDirSnapshot: ifo(%v) err(%v)", ifo, err)
		return err
	}

	return nil
}

func (mw *SnapShotMetaWrapper) DeleteDirSnapshot(ifo *proto.DirVerItem) (err error) {
	mp := mw.getPartitionByInode(ifo.RootIno)
	if mp == nil {
		log.LogErrorf("DeleteDirSnapshot: No such partition, ino(%v)", ifo)
		return syscall.EINVAL
	}

	err = mw.delDirSnapshot(mp, ifo)
	if err != nil {
		log.LogErrorf("DeleteDirSnapshot: ifo(%v) err(%v)", ifo, err)
		return err
	}

	return nil
}
