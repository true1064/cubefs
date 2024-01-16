package master

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

type DirToDelVerInfoByMpId struct {
	MetaPartitionId      uint64
	ToDelVerInfoByInoMap map[uint64]*proto.DelDirVersionInfo //key: inodes of dirs which have versions to delete.
}

func newDirToDelVerInfoByMpId(mpId uint64) *DirToDelVerInfoByMpId {
	return &DirToDelVerInfoByMpId{
		MetaPartitionId:      mpId,
		ToDelVerInfoByInoMap: make(map[uint64]*proto.DelDirVersionInfo),
	}
}

type DirDeletedVerInfoByMpId struct {
	MetaPartitionId      uint64
	DirDeletedVerInfoMap map[uint64]*proto.DirVerItem //key: inodes of dirs which have versions deleted.
}

func newDirDeletedVerInfoByMpId(metaPartitionId uint64) *DirDeletedVerInfoByMpId {
	return &DirDeletedVerInfoByMpId{
		MetaPartitionId:      metaPartitionId,
		DirDeletedVerInfoMap: make(map[uint64]*proto.DirVerItem),
	}
}

const (
	PreAllocSnapVerCount uint64 = 1000 * 1000
)

type DirSnapVerAllocator struct {
	PreAllocMaxVer uint64
	CurSnapVer     uint64
	sync.RWMutex
}

func newDirSnapVerAllocator() *DirSnapVerAllocator {
	return &DirSnapVerAllocator{
		PreAllocMaxVer: 0,
		CurSnapVer:     0,
	}
}

//PreAllocVersion :
// caller must handle the lock properly
func (dirVerAlloc *DirSnapVerAllocator) PreAllocVersion(vol *Vol, c *Cluster, nowMicroSec uint64) (err error) {
	if nowMicroSec <= dirVerAlloc.CurSnapVer {
		return fmt.Errorf("[PreAllocVersion] vol(%v) not allow pre alloc for nowMicroSec(%v ) <= CurSnapVer(%v)",
			vol.Name, nowMicroSec, dirVerAlloc.CurSnapVer)
	}

	if nowMicroSec <= dirVerAlloc.PreAllocMaxVer {
		return fmt.Errorf("[PreAllocVersion] vol(%v) not allow pre alloc for nowMicroSec(%v ) <= PreAllocMaxVer(%v)",
			vol.Name, nowMicroSec, dirVerAlloc.PreAllocMaxVer)
	}

	oldPreAllocMaxVer := dirVerAlloc.PreAllocMaxVer
	oldCurSnapVer := dirVerAlloc.CurSnapVer

	dirVerAlloc.PreAllocMaxVer = nowMicroSec + PreAllocSnapVerCount
	dirVerAlloc.CurSnapVer = nowMicroSec
	log.LogDebugf("[PreAllocVersion] vol(%v), alloc{CurSnapVer(%v), PreAllocMaxVer(%v)}, old{CurSnapVer(%v), PreAllocMaxVer(%v)}",
		vol.Name, dirVerAlloc.CurSnapVer, dirVerAlloc.PreAllocMaxVer, oldCurSnapVer, oldPreAllocMaxVer)

	return dirVerAlloc.Persist(vol, c)
}

func (dirVerAlloc *DirSnapVerAllocator) AllocVersion(vol *Vol, c *Cluster) (verInfo *proto.DirSnapshotVersionInfo, err error) {
	dirVerAlloc.Lock()
	defer dirVerAlloc.Unlock()

	nowMicroSec := uint64(time.Now().UnixMicro())

	if dirVerAlloc.CurSnapVer >= dirVerAlloc.PreAllocMaxVer {
		if err = dirVerAlloc.PreAllocVersion(vol, c, nowMicroSec); err != nil {
			return nil, err
		}
	}

	allocVer := uint64(0)
	if dirVerAlloc.CurSnapVer < nowMicroSec && nowMicroSec < dirVerAlloc.PreAllocMaxVer {
		allocVer = nowMicroSec
	} else {
		if dirVerAlloc.CurSnapVer >= nowMicroSec {
			allocVer = dirVerAlloc.CurSnapVer + 1
		} else if nowMicroSec >= dirVerAlloc.PreAllocMaxVer {
			allocVer = nowMicroSec
			if err = dirVerAlloc.PreAllocVersion(vol, c, nowMicroSec); err != nil {
				return nil, err
			}
		}
	}

	dirVerAlloc.CurSnapVer = allocVer
	return &proto.DirSnapshotVersionInfo{
		SnapVersion: allocVer,
	}, nil
}

type DirSnapVerAllocatorPersist struct {
	PreAllocMaxVer uint64
}

//Persist :
// caller must handle the lock properly
func (dirVerAlloc *DirSnapVerAllocator) Persist(vol *Vol, c *Cluster) (err error) {
	persist := DirSnapVerAllocatorPersist{
		PreAllocMaxVer: dirVerAlloc.PreAllocMaxVer,
	}

	err = c.syncDirVersion(vol, persist)
	return
}

func (dirVerAlloc *DirSnapVerAllocator) load(val []byte, volName string) (err error) {
	persistVer := &DirSnapVerAllocatorPersist{}
	if err = json.Unmarshal(val, persistVer); err != nil {
		return
	}

	dirVerAlloc.PreAllocMaxVer = persistVer.PreAllocMaxVer
	dirVerAlloc.CurSnapVer = persistVer.PreAllocMaxVer
	log.LogInfof("action[DirSnapVerAllocator.load]: vol[%v], PreAllocMaxVer: %v, CurSnapVer: %v",
		volName, dirVerAlloc.PreAllocMaxVer, dirVerAlloc.CurSnapVer)
	return nil
}

func (dirVerAlloc *DirSnapVerAllocator) init() {
	dirVerAlloc.Lock()
	defer dirVerAlloc.Unlock()

	dirVerAlloc.PreAllocMaxVer = 0
	dirVerAlloc.CurSnapVer = 0
	return
}

func (dirVerAlloc *DirSnapVerAllocator) String() string {
	dirVerAlloc.RLock()
	defer dirVerAlloc.RUnlock()

	return fmt.Sprintf("DirSnapVerAllocator:{ CurSnapVer[%v], PreAllocMaxVer[%v]}",
		dirVerAlloc.CurSnapVer, dirVerAlloc.PreAllocMaxVer)
}

type DirSnapVersionManager struct {
	vol *Vol
	c   *Cluster

	dirVerAllocator *DirSnapVerAllocator

	DeVerInfoLock sync.RWMutex

	// dir snap versions to delete, received from metaNode and waiting to notify lcNode
	// key: metaPartitionId
	toDelDirVerByMpIdMap map[uint64]*DirToDelVerInfoByMpId

	// ver infos deleted by lcNode and waiting to notify metaNode
	// key: metaPartitionId
	deletedDirVerByMpIdMap map[uint64]*DirDeletedVerInfoByMpId
}

func newDirSnapVersionManager(vol *Vol) *DirSnapVersionManager {
	return &DirSnapVersionManager{
		vol: vol,

		dirVerAllocator:        newDirSnapVerAllocator(),
		toDelDirVerByMpIdMap:   make(map[uint64]*DirToDelVerInfoByMpId),
		deletedDirVerByMpIdMap: make(map[uint64]*DirDeletedVerInfoByMpId),
	}
}

//TODO:tangjingyu del ver info
func (dirVerMgr *DirSnapVersionManager) String() string {
	return fmt.Sprintf("DirSnapVersionManager:{vol[%v], %v}",
		dirVerMgr.vol.Name, dirVerMgr.dirVerAllocator.String())
}

func (dirVerMgr *DirSnapVersionManager) SetCluster(c *Cluster) {
	dirVerMgr.c = c
	return
}

type DirToDelVersionInfoByMpIdPersist struct {
	MpId                uint64
	DirToDelVerInfoList []*proto.DelDirVersionInfo
}

func newDirToDelVersionInfoByMpIdPersist(toDelDirVersionInfo ToDelDirVersionInfoOfMp) *DirToDelVersionInfoByMpIdPersist {
	persist := &DirToDelVersionInfoByMpIdPersist{
		MpId:                toDelDirVersionInfo.MetaPartitionId,
		DirToDelVerInfoList: toDelDirVersionInfo.DirInfos,
	}

	return persist
}

type DirDeletedVerInfoByMpIdPersist struct {
	MpId                uint64
	DeletedVerByInoList []*proto.DirVerItem
}

func newDirDeletedVerInfoByMpIdPersist(mpId uint64) *DirDeletedVerInfoByMpIdPersist {
	return &DirDeletedVerInfoByMpIdPersist{
		MpId:                mpId,
		DeletedVerByInoList: make([]*proto.DirVerItem, 0),
	}
}

type DirDelVerInfoPersist struct {
	ToDelDirVersionInfoList []*DirToDelVersionInfoByMpIdPersist
	DeletedDirVerInfoList   []*DirDeletedVerInfoByMpIdPersist
}

func (dirVerMgr *DirSnapVersionManager) GetDirToDelVerInfoByMpIdPersist() []*DirToDelVersionInfoByMpIdPersist {
	toDelDirVersionInfoList := dirVerMgr.getToDelDirVersionInfoList()
	toDelDirVersionInfoListPersist := make([]*DirToDelVersionInfoByMpIdPersist, 0)

	for _, toDelVerInfo := range toDelDirVersionInfoList {
		toDelDirVersionInfoListPersist = append(toDelDirVersionInfoListPersist, newDirToDelVersionInfoByMpIdPersist(toDelVerInfo))
	}

	return toDelDirVersionInfoListPersist
}

func (dirVerMgr *DirSnapVersionManager) GetDirDeletedVerInfoByMpIdPersist() []*DirDeletedVerInfoByMpIdPersist {
	deletedDirVerInfoList := make([]*DirDeletedVerInfoByMpIdPersist, 0)

	for mpId, deletedVerInfoByMpId := range dirVerMgr.deletedDirVerByMpIdMap {
		log.LogDebugf("#### [GetDirDeletedVerInfoByMpIdPersist] mpId:%v, DirDeletedVerInfoMap len:%v",
			mpId, len(deletedVerInfoByMpId.DirDeletedVerInfoMap)) //TODO:tangjingyu del

		for _, deletedVerInfoByIno := range deletedVerInfoByMpId.DirDeletedVerInfoMap {
			log.LogInfof("#### [GetDirDeletedVerInfoByMpIdPersist] mpId:%v, deletedVerInfoByIno:%+v",
				mpId, deletedVerInfoByIno) //TODO:tangjingyu del

			dirDeletedVerInfoByMpIdPersist := newDirDeletedVerInfoByMpIdPersist(mpId)
			dirDeletedVerInfoByMpIdPersist.DeletedVerByInoList = append(dirDeletedVerInfoByMpIdPersist.DeletedVerByInoList, deletedVerInfoByIno)

			deletedDirVerInfoList = append(deletedDirVerInfoList, dirDeletedVerInfoByMpIdPersist)
			log.LogInfof("#### [GetDirDeletedVerInfoByMpIdPersist] appneded.... deletedDirVerInfoList: %+v", deletedDirVerInfoList) //TODO:tangjingyu string()
		}
	}

	log.LogInfof("#### [GetDirDeletedVerInfoByMpIdPersist] deletedDirVerInfoList len:%v", len(deletedDirVerInfoList)) //TODO:tangjingyu del
	log.LogInfof("#### [GetDirDeletedVerInfoByMpIdPersist] deletedDirVerInfoList: %+v", deletedDirVerInfoList)        //TODO:tangjingyu del
	return deletedDirVerInfoList
}

//PersistDirDelVerInfo :
// caller must handle the lock properly
func (dirVerMgr *DirSnapVersionManager) PersistDirDelVerInfo() (err error) {
	persist := &DirDelVerInfoPersist{
		ToDelDirVersionInfoList: dirVerMgr.GetDirToDelVerInfoByMpIdPersist(),
		DeletedDirVerInfoList:   dirVerMgr.GetDirDeletedVerInfoByMpIdPersist(),
	}

	var val []byte
	if val, err = json.Marshal(persist); err != nil {
		err = fmt.Errorf("[PersistDirDelVerInfo]: Marshal failed, vol: %v, err: %v", dirVerMgr.vol.Name, err)
		return
	}

	log.LogInfof("[PersistDirDelVerInfo] ToDelDirVersionInfoList len: %v, DeletedDirVerInfoList len: %v",
		len(persist.ToDelDirVersionInfoList), len(persist.DeletedDirVerInfoList))
	return dirVerMgr.c.syncDirDelVersionInfo(dirVerMgr.vol, val)
}

func (dirVerMgr *DirSnapVersionManager) loadDirVersionAllocator(val []byte) (err error) {
	return dirVerMgr.dirVerAllocator.load(val, dirVerMgr.vol.Name)
}

func (dirVerMgr *DirSnapVersionManager) loadDirDelVerInfo(val []byte) (err error) {
	persist := &DirDelVerInfoPersist{}
	if err = json.Unmarshal(val, persist); err != nil {
		return
	}

	//1. load to-del version info
	for idx, toDel := range persist.ToDelDirVersionInfoList {
		if toDel == nil {
			log.LogWarnf("[loadDirDelVerInfo] vol(%v), ToDelDirVersionInfoList idx[%v] is nil", dirVerMgr.vol.Name, idx)
			continue
		}

		log.LogDebugf("### [loadDirDelVerInfo] vol(%v), ToDelDirVersionInfoList idx[%v]: %#v", dirVerMgr.vol.Name, idx, toDel)
		err = dirVerMgr.AddDirToDelVerInfos(toDel.MpId, toDel.DirToDelVerInfoList, false)
		if err != nil {
			log.LogWarnf("[loadDirDelVerInfo] vol(%v) mpId(%v) AddDirToDelVerInfos (%#v) failed: %v",
				dirVerMgr.vol.Name, toDel.MpId, toDel.DirToDelVerInfoList, err.Error())
		}
	}

	//2. load deleted version info
	for _, deletedByMpId := range persist.DeletedDirVerInfoList {
		for _, deletedByIno := range deletedByMpId.DeletedVerByInoList {
			dirVerMgr.AddDirDeletedVer(deletedByMpId.MpId, deletedByIno.DirSnapIno, deletedByIno.RootIno, deletedByIno.Ver)
		}
	}

	log.LogInfof("action[loadDirDelVerInfo] vol[%v] load done, toDelListLen :%v, deletedListLen: %v",
		dirVerMgr.vol.Name, len(persist.ToDelDirVersionInfoList), len(persist.DeletedDirVerInfoList))
	return nil
}

func (dirVerMgr *DirSnapVersionManager) init(cluster *Cluster) error {
	log.LogWarnf("[DirSnapVersionManager] init, vol(%v)", dirVerMgr.vol.Name)
	dirVerMgr.SetCluster(cluster)

	dirVerMgr.dirVerAllocator.init()

	if cluster.partition.IsRaftLeader() {
		return dirVerMgr.dirVerAllocator.Persist(dirVerMgr.vol, cluster)
	}
	return nil
}

func (dirVerMgr *DirSnapVersionManager) AllocVersion() (verInfo *proto.DirSnapshotVersionInfo, err error) {
	return dirVerMgr.dirVerAllocator.AllocVersion(dirVerMgr.vol, dirVerMgr.c)
}

func (dirVerMgr *DirSnapVersionManager) AddDirToDelVerInfos(mpId uint64, addInfoList []*proto.DelDirVersionInfo, doPersist bool) (err error) {
	if _, err = dirVerMgr.vol.metaPartition(mpId); err != nil {
		err = fmt.Errorf("mpId(%v) not exist", mpId)
		return
	}

	addCnt := uint32(0)

	dirVerMgr.DeVerInfoLock.Lock()
	defer dirVerMgr.DeVerInfoLock.Unlock()

	var ok bool
	var dirToDelVerInfosOfMp *DirToDelVerInfoByMpId
	if dirToDelVerInfosOfMp, ok = dirVerMgr.toDelDirVerByMpIdMap[mpId]; !ok {
		dirToDelVerInfosOfMp = newDirToDelVerInfoByMpId(mpId)
		dirVerMgr.toDelDirVerByMpIdMap[mpId] = dirToDelVerInfosOfMp
		log.LogDebugf("[AddDirToDelVerInfos] add mpId(%v), vol(%v)", mpId, dirVerMgr.vol.Name)
	}

	defer func() {
		if dirToDelVerInfosOfMp == nil {
			return
		}

		if len(dirToDelVerInfosOfMp.ToDelVerInfoByInoMap) == 0 {
			log.LogDebugf("[AddDirToDelVerInfos] remove empty record of mpId(%v), vol(%)", mpId, dirVerMgr.vol.Name)
			delete(dirVerMgr.toDelDirVerByMpIdMap, mpId)
		}
	}()

	for _, addInfo := range addInfoList {
		if exist, ok := dirToDelVerInfosOfMp.ToDelVerInfoByInoMap[addInfo.DirIno]; ok {
			err = fmt.Errorf("dir already exists toDelVersion(%v), dirIno(%v)", (*exist).DelVer.DelVer, addInfo.DirIno)
			return
		}

		dirToDelVerInfosOfMp.ToDelVerInfoByInoMap[addInfo.DirIno] = addInfo
		log.LogDebugf("[AddDirToDelVerInfos] vol(%v) mpId(%v) add: dirIno(%v) toDelVer(%v)",
			mpId, dirVerMgr.vol.Name, addInfo.DirIno, addInfo.DelVer.DelVer)
		addCnt++
	}

	if addCnt == 0 {
		log.LogInfof("[AddDirToDelVerInfos] nothing changed, vol[%v] mpId[%v]", dirVerMgr.vol.Name, mpId)
		return
	}

	if !doPersist {
		log.LogInfof("[AddDirToDelVerInfos] vol[%v] mpId[%v] add count:%v", dirVerMgr.vol.Name, mpId, addCnt)
		return
	}

	if err = dirVerMgr.PersistDirDelVerInfo(); err != nil {
		err = fmt.Errorf("PersistDirDelVerInfo failed")
		log.LogErrorf("[AddDirToDelVerInfos] vol(%v) mpId(%v) persist failed, remove newly added record")
		for _, addInfo := range addInfoList {
			_ = dirVerMgr.RemoveDirToDelVer(mpId, addInfo.DirIno, addInfo.DelVer.DelVer)
		}
		return
	}
	log.LogInfof("[AddDirToDelVerInfos] vol[%v] mpId[%v] PersistDirDelVerInfo done, add count:%v",
		dirVerMgr.vol.Name, mpId, addCnt)
	return
}

type ToDelDirVersionInfoOfMp struct {
	VolName         string
	MetaPartitionId uint64
	DirInfos        []*proto.DelDirVersionInfo
}

//TODO:tangjingyu return pointer?
//for lcNode
func (dirVerMgr *DirSnapVersionManager) getToDelDirVersionInfoListWithLock() (toDelDirVersionInfoList []ToDelDirVersionInfoOfMp) {
	dirVerMgr.DeVerInfoLock.Lock()
	defer dirVerMgr.DeVerInfoLock.Unlock()

	return dirVerMgr.getToDelDirVersionInfoList()
}

func (dirVerMgr *DirSnapVersionManager) getToDelDirVersionInfoList() (toDelDirVersionInfoList []ToDelDirVersionInfoOfMp) {
	toDelDirVersionInfoList = make([]ToDelDirVersionInfoOfMp, 0)

	for mpId, dirToDelVerInfoByMpId := range dirVerMgr.toDelDirVerByMpIdMap {
		toDelDirVersionInfo := ToDelDirVersionInfoOfMp{
			VolName:         dirVerMgr.vol.Name,
			MetaPartitionId: dirToDelVerInfoByMpId.MetaPartitionId,
			DirInfos:        make([]*proto.DelDirVersionInfo, 0),
		}
		log.LogDebugf("#### [getToDelDirVersionInfoList] dirToDelVerInfoByMpId.ToDelVerInfoByInoMap len:%v",
			len(dirToDelVerInfoByMpId.ToDelVerInfoByInoMap)) //TODO:tangjignyu del

		for _, dirToDelVerInfo := range dirToDelVerInfoByMpId.ToDelVerInfoByInoMap {
			log.LogDebugf("[getToDelDirVersionInfoList] ranging, vol(%v) mpId(%v) dirToDelVerInfo: %v",
				dirVerMgr.vol.Name, mpId, dirToDelVerInfo.String())

			toDelDirVersionInfo.DirInfos = append(toDelDirVersionInfo.DirInfos, dirToDelVerInfo)
		}

		log.LogDebugf("[getToDelDirVersionInfoList] vol(%v) mpId(%v) got item cnt: %v",
			dirVerMgr.vol.Name, mpId, len(toDelDirVersionInfo.DirInfos))
		toDelDirVersionInfoList = append(toDelDirVersionInfoList, toDelDirVersionInfo)
	}

	log.LogDebugf("[getToDelDirVersionInfoList] vol[%v], got item cnt:%v",
		dirVerMgr.vol.Name, len(toDelDirVersionInfoList))
	return toDelDirVersionInfoList
}

// RemoveDirToDelVer :
// caller must handle the lock properly
// called when lcNode actually deleted the dir version
func (dirVerMgr *DirSnapVersionManager) RemoveDirToDelVer(metaPartitionId, dirIno uint64, deletedVer uint64) (ret *proto.DelDirVersionInfo) {
	var dirToDelVerInfoByMpId *DirToDelVerInfoByMpId
	var delDirVerInfo *proto.DelDirVersionInfo
	var ok bool

	if dirToDelVerInfoByMpId, ok = dirVerMgr.toDelDirVerByMpIdMap[metaPartitionId]; !ok {
		log.LogErrorf("[RemoveDirToDelVer]: vol[%v] not exist DirToDelVerInfoByMpId record with metaPartitionId=%v",
			dirVerMgr.vol.Name, metaPartitionId)
		return
	}

	if delDirVerInfo, ok = dirToDelVerInfoByMpId.ToDelVerInfoByInoMap[dirIno]; !ok {
		log.LogErrorf("[RemoveDirToDelVer]: vol[%v] not exist DirToDelVerInfosByIno record with dirInodeId=%v， metaPartitionId=%v",
			dirVerMgr.vol.Name, dirIno, metaPartitionId)
		return
	}

	if delDirVerInfo.DelVer.DelVer != deletedVer {
		log.LogErrorf("[RemoveDirToDelVer]: vol[%v] not exist to delete dir ver: %v, metaPartitionId=%v, dirInodeId=%v",
			dirVerMgr.vol.Name, deletedVer, metaPartitionId, dirIno)
		return
	}

	log.LogInfof("[RemoveDirToDelVer]: vol[%v], dirInodeId[%v] remove to delete dir ver: %v, metaPartitionId=%v",
		dirVerMgr.vol.Name, dirIno, deletedVer, metaPartitionId)
	ret = delDirVerInfo

	delete(dirToDelVerInfoByMpId.ToDelVerInfoByInoMap, dirIno)

	if len(dirToDelVerInfoByMpId.ToDelVerInfoByInoMap) == 0 {
		log.LogInfof("[RemoveDirToDelVer]: vol[%v] mpId[%v]: remove all versions to delete of metaPartition",
			dirVerMgr.vol.Name, metaPartitionId)
		delete(dirVerMgr.toDelDirVerByMpIdMap, metaPartitionId)
	}

	return
}

// AddDirDeletedVer :
// caller must handle the lock properly
// called when lcNode actually deleted the dir version
func (dirVerMgr *DirSnapVersionManager) AddDirDeletedVer(metaPartitionId, dirIno, subRootIno, deletedVer uint64) (err error) {
	var deletedVerInfoByIno *proto.DirVerItem
	var deletedVerInfoByMpId *DirDeletedVerInfoByMpId
	var ok bool

	if deletedVerInfoByMpId, ok = dirVerMgr.deletedDirVerByMpIdMap[metaPartitionId]; !ok {
		log.LogDebugf("[AddDirDeletedVer] vol(%v) has no record of mpId(%v), add it", dirVerMgr.vol.Name, metaPartitionId)
		deletedVerInfoByMpId = newDirDeletedVerInfoByMpId(metaPartitionId)
		dirVerMgr.deletedDirVerByMpIdMap[metaPartitionId] = deletedVerInfoByMpId
	}

	if deletedVerInfoByIno, ok = deletedVerInfoByMpId.DirDeletedVerInfoMap[dirIno]; ok {
		if deletedVerInfoByIno.Ver != deletedVer {
			err = fmt.Errorf("vol(%v) mpId(%v) already has deleted version record of dirIno(%v)",
				dirVerMgr.vol.Name, metaPartitionId, dirIno)
			log.LogErrorf("[AddDirDeletedVer] %v", err.Error())
			return
		}
	}

	log.LogDebugf("[AddDirDeletedVer]: vol[%v] mpId[%v] add record of dirIno(%v)",
		dirVerMgr.vol.Name, metaPartitionId, dirIno)
	deletedVerInfoByMpId.DirDeletedVerInfoMap[dirIno] = &proto.DirVerItem{
		DirSnapIno: dirIno,
		RootIno:    subRootIno,
		Ver:        deletedVer,
	}
	return
}

func (dirVerMgr *DirSnapVersionManager) DelVer(metaPartitionId, dirIno, deletedVer uint64) (err error) {
	dirVerMgr.DeVerInfoLock.Lock()
	defer dirVerMgr.DeVerInfoLock.Unlock()

	log.LogDebugf("[DelVer] vol[%v] mpId[%v]  dirIno[%v] del ver:%v",
		dirVerMgr.vol.Name, metaPartitionId, dirIno, deletedVer)

	toDelVerInfosByIno := dirVerMgr.RemoveDirToDelVer(metaPartitionId, dirIno, deletedVer)
	if toDelVerInfosByIno == nil {
		err = fmt.Errorf("vol(%v) mpId(%v) dirIno(%v) no record of del ver:%v",
			dirVerMgr.vol.Name, metaPartitionId, dirIno, deletedVer)
		log.LogErrorf("[DelVer] %v", err.Error())
		return
	}

	err = dirVerMgr.AddDirDeletedVer(metaPartitionId, dirIno, toDelVerInfosByIno.SubRootIno, toDelVerInfosByIno.DelVer.DelVer)
	if err != nil {
		log.LogErrorf("[DelVer] vol[%v] mpId[%v] dirIno[%v] already has record of deleted ver:%v",
			dirVerMgr.vol.Name, metaPartitionId, dirIno, deletedVer)
	}

	if toDelVerInfosByIno != nil {
		err = dirVerMgr.PersistDirDelVerInfo()
		//TODO:tangjingyu handle persisit failure
		// AddDirToDelVerInfos
		// RemoveDirDeletedVer
	}

	return
}

func (dirVerMgr *DirSnapVersionManager) RemoveDirDeletedVer(mpId uint64, deletedVers []proto.DirVerItem) (err error) {
	var (
		ok                   bool
		changed              bool
		deletedVerInfoByMpId *DirDeletedVerInfoByMpId
	)
	log.LogDebugf("[RemoveDirDeletedVer] vol[%v] mpId[%v] deletedVers:+%v", dirVerMgr.vol.Name, mpId, deletedVers)

	if len(deletedVers) == 0 {
		err = fmt.Errorf("deleted ver count is 0")
		log.LogErrorf("[RemoveDirDeletedVer] vol[%v] mpId[%v] err:%v", dirVerMgr.vol.Name, mpId, err.Error())
		return
	}

	dirVerMgr.DeVerInfoLock.Lock()
	defer dirVerMgr.DeVerInfoLock.Unlock()

	if deletedVerInfoByMpId, ok = dirVerMgr.deletedDirVerByMpIdMap[mpId]; !ok {
		err = fmt.Errorf("no record of mpId")
		log.LogErrorf("[RemoveDirDeletedVer] vol[%v] mpId[%v] err:%v", dirVerMgr.vol.Name, mpId, err.Error())
		return
	}

	for _, verItem := range deletedVers {
		var deletedVerInfoByIno *proto.DirVerItem
		if deletedVerInfoByIno, ok = deletedVerInfoByMpId.DirDeletedVerInfoMap[verItem.DirSnapIno]; !ok {
			log.LogErrorf("[RemoveDirDeletedVer] vol[%v] mpId[%v] no record of dirInodeId:%v",
				dirVerMgr.vol.Name, mpId, verItem.DirSnapIno)
			continue
		}

		if deletedVerInfoByIno.Ver != verItem.Ver {
			log.LogErrorf("[RemoveDirDeletedVer] vol[%v] mpId[%v] dirInodeId[%v]: ver not match, recordVer(%v), reqVer(%v)",
				dirVerMgr.vol.Name, mpId, verItem.DirSnapIno, deletedVerInfoByIno.Ver, verItem.Ver)
			continue
		}

		delete(deletedVerInfoByMpId.DirDeletedVerInfoMap, verItem.DirSnapIno)
		changed = true
		log.LogDebugf("[RemoveDirDeletedVer] vol[%v] mpId[%v] clean record of dirInodeId:%v",
			dirVerMgr.vol.Name, mpId, verItem.DirSnapIno)

		if len(deletedVerInfoByMpId.DirDeletedVerInfoMap) == 0 {
			delete(dirVerMgr.deletedDirVerByMpIdMap, mpId)
			log.LogDebugf("[RemoveDirDeletedVer] vol[%v] clean record of mpId:%v",
				dirVerMgr.vol.Name, mpId)
		}
	}

	if !changed {
		log.LogInfof("[RemoveDirDeletedVer] vol[%v] mpId[%v] try deleted ver count:%v, but nothing changed",
			dirVerMgr.vol.Name, mpId, len(deletedVers))
		return
	}

	if err = dirVerMgr.PersistDirDelVerInfo(); err != nil {
		log.LogErrorf("[RemoveDirDeletedVer] PersistDirDelVerInfo failed, err:%v, vol:%v, MpId:%v",
			err.Error(), dirVerMgr.vol.Name, mpId)
	}
	return
}

func (dirVerMgr *DirSnapVersionManager) ReqMetaNodeToBatchDelDirSnapVer(mpId uint64, deletedVers []proto.DirVerItem) (err error) {
	var (
		mp *MetaPartition
		mr *MetaReplica
	)

	log.LogDebugf("[ReqMetaNodeToBatchDelDirSnapVer] vol[%v] mpId[%v] deletedVers: %+v",
		dirVerMgr.vol.Name, mpId, deletedVers)

	if mp, err = dirVerMgr.c.getMetaPartitionByID(mpId); err != nil {
		log.LogErrorf("[ReqMetaNodeToBatchDelDirSnapVer] err:%v, vol:%v, mpId:%v",
			err.Error(), dirVerMgr.vol.Name, mpId)
		return
	}

	if mr, err = mp.getMetaReplicaLeader(); err != nil {
		log.LogErrorf("[ReqMetaNodeToBatchDelDirSnapVer] get MetaReplica leader fail, err:%v, vol:%v, MpId:%v",
			err.Error(), dirVerMgr.vol.Name, mpId)
		return
	}

	task := mr.metaNode.createDirVerDelTask(dirVerMgr.vol.Name, mpId, deletedVers)
	if _, err = mr.metaNode.Sender.syncSendAdminTask(task); err != nil {
		log.LogErrorf("[ReqMetaNodeToBatchDelDirSnapVer] req metaNode(%v) batch del dir ver failed, err:%v, vol:%v, mpId:%v",
			mr.Addr, err.Error(), dirVerMgr.vol.Name, mpId)
		return
	}

	if err = dirVerMgr.RemoveDirDeletedVer(mpId, deletedVers); err != nil {
		log.LogErrorf("[ReqMetaNodeToBatchDelDirSnapVer] RemoveDirDeletedVer failed, err:%v, vol:%v, mpId:%v",
			mr.Addr, err.Error(), dirVerMgr.vol.Name, mpId)
		return
	}

	return
}

func (dirVerMgr *DirSnapVersionManager) CheckDirDeletedVer() {
	length := len(dirVerMgr.deletedDirVerByMpIdMap)
	if length > 0 {
		log.LogDebugf("[CheckDirDeletedVer] vol[%v] to handle deletedDirVerByMpIdMap len:%v",
			dirVerMgr.vol.Name, length)

	}
	deletedDirVerByMpIdMapCopy := make(map[uint64]*DirDeletedVerInfoByMpId)

	//copy for preventing network requests from occupying the lock
	dirVerMgr.DeVerInfoLock.RLock()
	for mpId, deletedDirVerByMpId := range dirVerMgr.deletedDirVerByMpIdMap {
		deletedVerInfoByMpIdCopy := newDirDeletedVerInfoByMpId(mpId)

		for dirInodeId, deletedVerInfoByIno := range deletedDirVerByMpId.DirDeletedVerInfoMap {
			deletedVerInfoByMpIdCopy.DirDeletedVerInfoMap[dirInodeId] = deletedVerInfoByIno
		}

		deletedDirVerByMpIdMapCopy[mpId] = deletedVerInfoByMpIdCopy
	}
	dirVerMgr.DeVerInfoLock.RUnlock()

	for mpId, deletedDirVerByMpId := range deletedDirVerByMpIdMapCopy {
		deletedVerList := make([]proto.DirVerItem, 0)
		for _, deletedVerInfoByIno := range deletedDirVerByMpId.DirDeletedVerInfoMap {
			deletedVerList = append(deletedVerList, *deletedVerInfoByIno)
		}

		if err := dirVerMgr.ReqMetaNodeToBatchDelDirSnapVer(mpId, deletedVerList); err != nil {
			log.LogErrorf("[CheckDirDeletedVer] failed to create batch del task to metaNode, err:%v, vol:%v, mpId:%v",
				err.Error(), dirVerMgr.vol.Name, mpId)
			continue
		}
		log.LogInfof("[CheckDirDeletedVer] vol[%v] mpId[%v] batch del task to metaNode done, deletedVerList len: %v",
			dirVerMgr.vol.Name, mpId, len(deletedVerList))
	}

	return
}

type DirSnapVerAllocatorValue struct {
	PreAllocMaxVer uint64
	CurSnapVer     uint64
}

type DirSnapVersionInfoValue struct {
	AllocatorVal            *DirSnapVerAllocatorValue
	ToDelDirVersionInfoList []*DirToDelVersionInfoByMpIdPersist
	DeletedDirVerInfoList   []*DirDeletedVerInfoByMpIdPersist
}

func (dirVerMgr *DirSnapVersionManager) Query() *DirSnapVersionInfoValue {
	dirVerMgr.DeVerInfoLock.RLock()

	defer dirVerMgr.DeVerInfoLock.RUnlock()
	allocVal := &DirSnapVerAllocatorValue{
		PreAllocMaxVer: dirVerMgr.dirVerAllocator.PreAllocMaxVer,
		CurSnapVer:     dirVerMgr.dirVerAllocator.CurSnapVer,
	}

	allocVal.PreAllocMaxVer = dirVerMgr.dirVerAllocator.PreAllocMaxVer

	dirVerMgr.getToDelDirVersionInfoList()

	retVal := &DirSnapVersionInfoValue{
		AllocatorVal:            allocVal,
		ToDelDirVersionInfoList: dirVerMgr.GetDirToDelVerInfoByMpIdPersist(),
		DeletedDirVerInfoList:   dirVerMgr.GetDirDeletedVerInfoByMpIdPersist(),
	}

	return retVal
}
