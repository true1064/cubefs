package master

import (
	"fmt"
	//"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/proto"
	"sync"
)

type WaitDelDirVerInfos struct {
	mpDelDirVerInfoMapLock sync.RWMutex
}

type DirSnapVerManager struct {
	vol       *Vol
	snapVerID uint64
	svIDLock  sync.RWMutex

	//	// dir snap versions to delete, sent by metaNode,
	//	// the first-level key: metaPartitionId; the second-level key: DirInode
	//	ToDelDirVerInfoMap  map[uint64]map[uint64][]proto.DelVer
	//	ToDelDirVerInfoLock sync.RWMutex
	//
	//	// deleted dir snap versions and waiting to report to metaNode
	//	// the first-level key: metaPartitionId; the second-level key: DirInode
	//	DeletedDirVerInfoMap  map[uint64]map[uint64][]proto.DelVer
	//	DeletedDirVerInfoLock sync.RWMutex
}

func newDirSnapVerManager(vol *Vol) *DirSnapVerManager {
	return &DirSnapVerManager{
		vol:       vol,
		snapVerID: 0,

		//ToDelDirVerInfoMap:   make(map[uint64]map[uint64][]proto.DelVer),
		//DeletedDirVerInfoMap: make(map[uint64]map[uint64][]proto.DelVer),
	}
}

func (verMgr *DirSnapVerManager) String() string {
	return fmt.Sprintf("DirSnapVerManager:{vol[%v], snapVerId[%v]",
		verMgr.vol.Name, verMgr.snapVerID)
}

func (verMgr *DirSnapVerManager) Persist() (err error) {

	return
}

func (verMgr *DirSnapVerManager) loadMultiVersion(c *Cluster, val []byte) (err error) {

	return nil
}

func (verMgr *DirSnapVerManager) init(cluster *Cluster) error {

	return nil
}

func (verMgr *DirSnapVerManager) AllocVersion() (verInfo *proto.DirSnapshotVersionInfo, err error) {
	//TODO:tangjingyu 预分配多个，比如1000个，以及持久化
	return &proto.DirSnapshotVersionInfo{
		SnapVersion: verMgr.snapVerID + 1,
	}, nil
}

func (verMgr *DirSnapVerManager) AddToDelDirVerInfos(mpId uint64, infoList []proto.DelDirVersionInfo) (err error) {
	//verMgr.ToDelDirVerInfoLock.Lock()
	//defer verMgr.ToDelDirVerInfoLock.Unlock()
	//
	//var dirInoDelVerMap map[uint64][]proto.DelVer
	//if dirInoDelVerMap, ok := verMgr.ToDelDirVerInfoMap[mpId]; !ok {
	//	dirInoDelVerMap = make(map[uint64][]proto.DelVer)
	//	verMgr.ToDelDirVerInfoMap[mpId] = dirInoDelVerMap
	//}
	//
	//for _, info := range infoList {
	//	var delDirInfoList []proto.DelVer
	//	if delDirInfoList, ok := dirInoDelVerMap[info.DirIno]; !ok {
	//		delDirInfoList = make([]proto.DelVer, len(info.DelVers))
	//		dirInoDelVerMap[info.DirIno] = delDirInfoList
	//	}
	//
	//	delDirInfoList = append(delDirInfoList, info.DelVers...)
	//}
	//
	////TODO: persist
	return
}

type ToDelDirVersionInfo struct {
	VolName         string
	MetaPartitionId uint64
	DirInfos        []proto.DelDirVersionInfo
}

func (verMgr *DirSnapVerManager) getToDelDirVersionInfoList() (ToDelDirVersionInfoList []ToDelDirVersionInfo) {
	//verMgr.ToDelDirVerInfoLock.RLock()
	//defer verMgr.ToDelDirVerInfoLock.RUnlock()
	//
	//toDelDirVerInfoMap = make(map[uint64]map[uint64][]proto.DelVer)
	//for mpId, dirInoDelVerMap := range verMgr.ToDelDirVerInfoMap {
	//	toDelDirVerInfoMap[mpId] = make(map[uint64][]proto.DelVer)
	//
	//	for dirIno, delDirVerInfoList := range dirInoDelVerMap {
	//		copyDelDirVerInfoList := make([]proto.DelVer, len(delDirVerInfoList))
	//		_ = copy(copyDelDirVerInfoList, delDirVerInfoList)
	//		toDelDirVerInfoMap[mpId][dirIno] = copyDelDirVerInfoList
	//	}
	//}
	//
	ToDelDirVersionInfoList = make([]ToDelDirVersionInfo, 0)
	return ToDelDirVersionInfoList
}

func (verMgr *DirSnapVerManager) DelVer(metaPartitionId, dirIno uint64, verSeq uint64) (err error) {
	//var dirInoDelVerMap map[uint64][]proto.DelVer
	//var delDirVerInfoList []proto.DelVer
	//var ok bool
	//
	//verMgr.ToDelDirVerInfoLock.Lock()
	//defer verMgr.ToDelDirVerInfoLock.Unlock()
	//
	//if dirInoDelVerMap, ok = verMgr.ToDelDirVerInfoMap[metaPartitionId]; !ok {
	//	return fmt.Errorf("not exist record with metaPartitionId=%v", metaPartitionId)
	//}
	//
	//if delDirVerInfoList, ok = dirInoDelVerMap[dirIno]; !ok {
	//	return fmt.Errorf("not exist record with metaPartitionId=%v and dirInodeId=%v", metaPartitionId, dirIno)
	//}
	//
	//foundIdx := int(-1)
	//for idx, info := range delDirVerInfoList {
	//	if info.DelVer == verSeq {
	//		foundIdx = idx
	//	}
	//}
	//
	//if foundIdx == -1 {
	//	return fmt.Errorf("not exist record with DelVer=%v, metaPartitionId=%v dirInodeId=%v",
	//		verSeq, metaPartitionId, dirIno)
	//}
	//
	//delDirVerInfoList = append(delDirVerInfoList[:foundIdx], delDirVerInfoList[foundIdx+1:]...)
	//
	////TODO: persist
	return nil
}
