package master

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"sort"
	"sync"
)

type DirToDelVerInfosByIno struct {
	DirInode   uint64
	SubRootIno uint64

	ToDelVerSet map[uint64]struct{}  //key: DelVer
	Vers        []*proto.VersionInfo //all versions of the dir
}

func newDirToDelVerInfos(dirInode, subRootIno uint64) *DirToDelVerInfosByIno {
	return &DirToDelVerInfosByIno{
		DirInode:   dirInode,
		SubRootIno: subRootIno,

		ToDelVerSet: make(map[uint64]struct{}),
		Vers:        make([]*proto.VersionInfo, 0),
	}
}

func (d *DirToDelVerInfosByIno) AddDirToDelVers(delVers []proto.DelVer) {
	for _, delVer := range delVers {
		if _, ok := d.ToDelVerSet[delVer.DelVer]; !ok {
			d.ToDelVerSet[delVer.DelVer] = struct{}{}
			log.LogInfof("[AddDirToDelVers] add DelVer:%v, DirInode:%v, SubRootIno:%v",
				delVer.DelVer, d.DirInode, d.SubRootIno)
		} else {
			log.LogInfof("[AddDirToDelVers] DelVer:%v already exists, DirInode:%v, SubRootIno:%v",
				delVer.DelVer, d.DirInode, d.SubRootIno)
		}

		//TODO:如果同一个dir收到多次请求，需要把每个请求里的Vers合并吗？
		d.Vers = make([]*proto.VersionInfo, len(delVer.Vers))
		copy(d.Vers, delVer.Vers)
	}
}

type DirToDelVerInfoByMpId struct {
	MetaPartitionId       uint64
	DirDelVerInfoByInoMap map[uint64]*DirToDelVerInfosByIno //key: inodes of dirs which have versions to delete.
}

func newDirToDelVerInfoByMpId(mpId uint64) *DirToDelVerInfoByMpId {
	return &DirToDelVerInfoByMpId{
		MetaPartitionId:       mpId,
		DirDelVerInfoByInoMap: make(map[uint64]*DirToDelVerInfosByIno),
	}
}

type DirDeletedVerInfoByIno struct {
	DirInode   uint64
	SubRootIno uint64

	DeletedVerSet map[uint64]struct{} //key: Deleted Version
}

func newDirDeletedVerInfos(dirInode, subRootIno uint64) *DirDeletedVerInfoByIno {
	return &DirDeletedVerInfoByIno{
		DirInode:      dirInode,
		SubRootIno:    subRootIno,
		DeletedVerSet: make(map[uint64]struct{}),
	}
}

type DirDeletedVerInfoByMpId struct {
	MetaPartitionId        uint64
	DirDeletedVerInfoByIno map[uint64]*DirDeletedVerInfoByIno //key: inodes of dirs which have versions deleted.
}

//TODO:
//func newDirDeletedVerInfoByMpId(mpId uint64) *DirDeletedVerInfoByMpId {
//	return &DirDeletedVerInfoByMpId{
//		MetaPartitionId:        mpId,
//		DirDeletedVerInfoByIno: make(map[uint64]*DirDeletedVerInfoByIno),
//	}
//}

const (
	PreAllocSnapVerCount uint64 = 1000
)

type DirSnapVersionManager struct {
	vol *Vol
	c   *Cluster

	//enabled bool //TODO: aad a configurable switch

	PreAllocSnapVer uint64
	CurSnapVer      uint64
	svIDLock        sync.RWMutex

	// dir snap versions to delete, received from metaNode. key: metaPartitionId
	toDelDirVerInfoMap  map[uint64]*DirToDelVerInfoByMpId
	toDelDirVerInfoLock sync.RWMutex

	// key: metaPartitionId
	deletedDirVerInfoMap map[uint64]*DirDeletedVerInfoByIno
	//deletedDirVerInfoLock sync.RWMutex //TODO: need this?
}

func newSnapVersionManager(vol *Vol) *DirSnapVersionManager {
	return &DirSnapVersionManager{
		vol:             vol,
		PreAllocSnapVer: 0,
		CurSnapVer:      0,

		toDelDirVerInfoMap:   make(map[uint64]*DirToDelVerInfoByMpId),
		deletedDirVerInfoMap: make(map[uint64]*DirDeletedVerInfoByIno),
	}
}

func (dirVerMgr *DirSnapVersionManager) String() string {
	return fmt.Sprintf("DirSnapVersionManager:{vol[%v], snapVerId[%v]",
		dirVerMgr.vol.Name, dirVerMgr.CurSnapVer)
}

type DirVersionPersist struct {
	PreAllocSnapVer uint64
}

func (direrMgr *DirSnapVersionManager) PersistSnapVer() (err error) {
	persistVer := &DirVersionPersist{
		PreAllocSnapVer: direrMgr.PreAllocSnapVer,
	}
	var val []byte
	if val, err = json.Marshal(persistVer); err != nil {
		return
	}

	err = direrMgr.c.syncDirVersion(direrMgr.vol, val)
	return
}

func (dirVerMgr *DirSnapVersionManager) loadDirVersion(c *Cluster, val []byte) (err error) {
	persistVer := &DirVersionPersist{}
	dirVerMgr.c = c
	if err = json.Unmarshal(val, persistVer); err != nil {
		return
	}

	dirVerMgr.PreAllocSnapVer = persistVer.PreAllocSnapVer
	dirVerMgr.CurSnapVer = dirVerMgr.PreAllocSnapVer
	return nil
}

func (direrMgr *DirSnapVersionManager) PersistDirDelVerInfo() (err error) {

	return
}

func (dirVerMgr *DirSnapVersionManager) init(cluster *Cluster) error {
	log.LogWarnf("action[DirSnapVersionManager.init] vol %v", dirVerMgr.vol.Name)
	dirVerMgr.c = cluster
	dirVerMgr.CurSnapVer = 0

	if cluster.partition.IsRaftLeader() {
		return dirVerMgr.PersistSnapVer()
	}
	return nil
}

//PreAllocVersion :
// caller must handle the lock properly
func (dirVerMgr *DirSnapVersionManager) PreAllocVersion() (err error) {
	dirVerMgr.PreAllocSnapVer = dirVerMgr.PreAllocSnapVer + PreAllocSnapVerCount
	return dirVerMgr.PersistSnapVer()
}

func (dirVerMgr *DirSnapVersionManager) AllocVersion() (verInfo *proto.DirSnapshotVersionInfo, err error) {
	//TDOO: lock scope
	dirVerMgr.svIDLock.Lock()
	defer dirVerMgr.svIDLock.Unlock()

	if dirVerMgr.CurSnapVer == dirVerMgr.PreAllocSnapVer {
		if err = dirVerMgr.PersistSnapVer(); err != nil {
			return nil, err
		}
	}
	dirVerMgr.CurSnapVer = dirVerMgr.CurSnapVer + 1

	return &proto.DirSnapshotVersionInfo{
		SnapVersion: dirVerMgr.CurSnapVer,
	}, nil
}

func (dirVerMgr *DirSnapVersionManager) AddDirToDelVerInfos(mpId uint64, infoList []proto.DelDirVersionInfo) (err error) {
	var changed bool

	dirVerMgr.toDelDirVerInfoLock.Lock()
	defer dirVerMgr.toDelDirVerInfoLock.Unlock()

	var ok bool
	var dirToDelVerInfosOfMp *DirToDelVerInfoByMpId
	if dirToDelVerInfosOfMp, ok = dirVerMgr.toDelDirVerInfoMap[mpId]; !ok {
		dirToDelVerInfosOfMp = newDirToDelVerInfoByMpId(mpId)
		dirVerMgr.toDelDirVerInfoMap[mpId] = dirToDelVerInfosOfMp
	}

	for _, info := range infoList {
		if len(info.DelVers) == 0 {
			log.LogErrorf("[AddDirToDelVerInfos]: len(DelDirVersionInfo.DelVers) is 0, dirInode:%v, mpId:%v, ",
				info.DirIno, mpId)
			continue
		}

		var dirToDelVerInfos *DirToDelVerInfosByIno
		if dirToDelVerInfos, ok = dirToDelVerInfosOfMp.DirDelVerInfoByInoMap[info.DirIno]; !ok {
			dirToDelVerInfos = newDirToDelVerInfos(info.DirIno, info.SubRootIno)
		}

		dirToDelVerInfos.AddDirToDelVers(info.DelVers)
		changed = true
	}

	if changed {
		err = dirVerMgr.PersistDirDelVerInfo()
	} else {
		log.LogInfof("[AddDirToDelVerInfos]: nothing changed, mpId:%v, ", mpId)
	}

	return
}

type ToDelDirVersionInfo struct {
	VolName         string
	MetaPartitionId uint64
	DirInfos        []proto.DelDirVersionInfo
}

//for lcNode
func (dirVerMgr *DirSnapVersionManager) getToDelDirVersionInfoList() (toDelDirVersionInfoList []ToDelDirVersionInfo) {
	toDelDirVersionInfoList = make([]ToDelDirVersionInfo, 0)

	//TODO: lock scope
	dirVerMgr.toDelDirVerInfoLock.Lock()
	defer dirVerMgr.toDelDirVerInfoLock.Unlock()

	for _, dirToDelVerInfoByMpId := range dirVerMgr.toDelDirVerInfoMap {
		toDelDirVersionInfo := ToDelDirVersionInfo{
			VolName:         dirVerMgr.vol.Name,
			MetaPartitionId: dirToDelVerInfoByMpId.MetaPartitionId,
			DirInfos:        make([]proto.DelDirVersionInfo, 0),
		}

		for _, dirToDelVerInfos := range dirToDelVerInfoByMpId.DirDelVerInfoByInoMap {
			delDirVersionInfo := proto.DelDirVersionInfo{
				DirIno:     dirToDelVerInfos.DirInode,
				SubRootIno: dirToDelVerInfos.SubRootIno,
				DelVers:    make([]proto.DelVer, len(dirToDelVerInfos.ToDelVerSet)),
			}

			for verToDel := range dirToDelVerInfos.ToDelVerSet {
				delVer := proto.DelVer{
					DelVer: verToDel,
					Vers:   make([]*proto.VersionInfo, len(dirToDelVerInfos.Vers)),
				}
				copy(delVer.Vers, dirToDelVerInfos.Vers) //TODO: use ptr?

				delDirVersionInfo.DelVers = append(delDirVersionInfo.DelVers, delVer)
				sort.SliceStable(delDirVersionInfo.DelVers, func(i, j int) bool {
					return delDirVersionInfo.DelVers[i].DelVer < delDirVersionInfo.DelVers[j].DelVer
				})
			}

			toDelDirVersionInfo.DirInfos = append(toDelDirVersionInfo.DirInfos, delDirVersionInfo)
		}

		//TODO: log
		toDelDirVersionInfoList = append(toDelDirVersionInfoList, toDelDirVersionInfo)
	}

	return toDelDirVersionInfoList
}

// RemoveDirToDelVer :
// caller must handle the lock properly
// called when lcNode actually deleted the dir version
func (dirVerMgr *DirSnapVersionManager) RemoveDirToDelVer(metaPartitionId, dirIno uint64, deletedVer uint64) (changed bool) {
	var dirToDelVerInfoByMpId *DirToDelVerInfoByMpId
	var dirToDelVerInfos *DirToDelVerInfosByIno
	var ok bool
	changed = false

	if dirToDelVerInfoByMpId, ok = dirVerMgr.toDelDirVerInfoMap[metaPartitionId]; !ok {
		log.LogErrorf("[RemoveDirToDelVer]: vol[%v] not exist DirToDelVerInfoByMpId record with metaPartitionId=%v",
			dirVerMgr.vol.Name, metaPartitionId)
		return
	}

	if dirToDelVerInfos, ok = dirToDelVerInfoByMpId.DirDelVerInfoByInoMap[dirIno]; !ok {
		log.LogErrorf("[RemoveDirToDelVer]: vol[%v] not exist DirToDelVerInfosByIno record with dirInodeId=%v， metaPartitionId=%v",
			dirVerMgr.vol.Name, dirIno, metaPartitionId)
		return
	}

	if _, ok = dirToDelVerInfos.ToDelVerSet[deletedVer]; !ok {
		log.LogErrorf("[RemoveDirToDelVer]: vol[%v] not exist to delete dir ver: %v, metaPartitionId=%v, dirInodeId=%v",
			dirVerMgr.vol.Name, deletedVer, metaPartitionId, dirIno)
		return
	}
	delete(dirToDelVerInfos.ToDelVerSet, deletedVer)
	log.LogInfof("[RemoveDirToDelVer]: vol[%v], dirInodeId[%v] remove to delete dir ver: %v, metaPartitionId=%v",
		dirVerMgr.vol.Name, dirIno, deletedVer, metaPartitionId)
	changed = true

	if len(dirToDelVerInfos.ToDelVerSet) == 0 {
		log.LogInfof("[RemoveDirToDelVer]: vol[%v], dirInodeId[%v] remove all versions to delete, latest remove ver: %v, metaPartitionId=%v",
			dirVerMgr.vol.Name, dirIno, deletedVer, metaPartitionId)
		delete(dirToDelVerInfoByMpId.DirDelVerInfoByInoMap, dirIno)
	}
	return
}

// AddDirDeletedVer :
// caller must handle the lock properly
// called when lcNode actually deleted the dir version
func (dirVerMgr *DirSnapVersionManager) AddDirDeletedVer(metaPartitionId, dirIno, deletedVer uint64) (changed bool) {
	var deletedVerInfoByIno *DirDeletedVerInfoByIno
	var ok bool

	if deletedVerInfoByIno, ok = dirVerMgr.deletedDirVerInfoMap[metaPartitionId]; !ok {
		log.LogDebugf("[AddDirDeletedVer]: vol[%v] has no record with metaPartitionId=%v, dirInodeId=%v",
			dirVerMgr.vol.Name, metaPartitionId, dirIno)
		deletedVerInfoByIno = newDirDeletedVerInfos(dirIno, deletedVer)
		dirVerMgr.deletedDirVerInfoMap[metaPartitionId] = deletedVerInfoByIno
	}

	if _, ok = deletedVerInfoByIno.DeletedVerSet[deletedVer]; ok {
		log.LogInfo("[AddDirDeletedVer]: vol[%v], dirInodeId[%v] already exists deletedVer: %v, metaPartitionId=%v",
			dirVerMgr.vol.Name, dirIno, deletedVer, metaPartitionId)
		changed = false
	} else {
		deletedVerInfoByIno.DeletedVerSet[deletedVer] = struct{}{}
		changed = true
	}

	return
}

func (dirVerMgr *DirSnapVersionManager) DelVer(metaPartitionId, dirIno, deletedVer uint64) (err error) {
	//TODO: lock scope
	dirVerMgr.toDelDirVerInfoLock.Lock()
	defer dirVerMgr.toDelDirVerInfoLock.Unlock()

	removedToDelVer := dirVerMgr.RemoveDirToDelVer(metaPartitionId, dirIno, deletedVer)

	addedDeletedVer := dirVerMgr.AddDirDeletedVer(metaPartitionId, dirIno, deletedVer)

	if removedToDelVer || addedDeletedVer {
		err = dirVerMgr.PersistDirDelVerInfo()
	}

	return
}
