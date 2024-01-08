package master

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
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
	MetaPartitionId      uint64
	ToDelVerInfoByInoMap map[uint64]*DirToDelVerInfosByIno //key: inodes of dirs which have versions to delete.
}

func newDirToDelVerInfoByMpId(mpId uint64) *DirToDelVerInfoByMpId {
	return &DirToDelVerInfoByMpId{
		MetaPartitionId:      mpId,
		ToDelVerInfoByInoMap: make(map[uint64]*DirToDelVerInfosByIno),
	}
}

type DirDeletedVerInfoByIno struct {
	DirInode      uint64
	SubRootIno    uint64
	DeletedVerSet map[uint64]struct{} //key: Deleted Version
}

func newDirDeletedVerInfoByIno(dirInode, subRootIno uint64) *DirDeletedVerInfoByIno {
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

func newDirDeletedVerInfoByMpId(metaPartitionId uint64) *DirDeletedVerInfoByMpId {
	return &DirDeletedVerInfoByMpId{
		MetaPartitionId:        metaPartitionId,
		DirDeletedVerInfoByIno: make(map[uint64]*DirDeletedVerInfoByIno),
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
	//enabled bool //TODO: aad a configurable switch

	dirVerAllocator *DirSnapVerAllocator

	DeVerInfoLock sync.RWMutex //TODO:tangjingyu name of the lock
	// dir snap versions to delete, received from metaNode. key: metaPartitionId
	toDelDirVerByMpIdMap map[uint64]*DirToDelVerInfoByMpId

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

//TODO: del ver info
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
	DirToDelVerInfoList []proto.DelDirVersionInfo //TODO: change to pointer
}

func newDirToDelVersionInfoByMpIdPersist(toDelDirVersionInfo ToDelDirVersionInfo) *DirToDelVersionInfoByMpIdPersist {
	persist := &DirToDelVersionInfoByMpIdPersist{
		MpId:                toDelDirVersionInfo.MetaPartitionId,
		DirToDelVerInfoList: make([]proto.DelDirVersionInfo, len(toDelDirVersionInfo.DirInfos)),
	}

	copy(persist.DirToDelVerInfoList, toDelDirVersionInfo.DirInfos)
	return persist
}

type DirDeletedVerInfoByInoPersist struct {
	DirInode       uint64
	SubRootIno     uint64
	DeletedVerList []uint64
}

func newDirDeletedVerInfoByInoPersist(dInfo *DirDeletedVerInfoByIno) *DirDeletedVerInfoByInoPersist {
	dirDeletedVerInfoByInoPersist := &DirDeletedVerInfoByInoPersist{
		DirInode:       dInfo.DirInode,
		SubRootIno:     dInfo.SubRootIno,
		DeletedVerList: make([]uint64, 0),
	}

	for deletedVer := range dInfo.DeletedVerSet {
		dirDeletedVerInfoByInoPersist.DeletedVerList = append(dirDeletedVerInfoByInoPersist.DeletedVerList, deletedVer)
	}

	return dirDeletedVerInfoByInoPersist
}

type DirDeletedVerInfoByMpIdPersist struct {
	MpId                uint64
	DeletedVerByInoList []*DirDeletedVerInfoByInoPersist
}

func newDirDeletedVerInfoByMpIdPersist(mpId uint64) *DirDeletedVerInfoByMpIdPersist {
	return &DirDeletedVerInfoByMpIdPersist{
		MpId:                mpId,
		DeletedVerByInoList: make([]*DirDeletedVerInfoByInoPersist, 0),
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
		log.LogDebugf("#### [GetDirDeletedVerInfoByMpIdPersist] mpId:%v, DirDeletedVerInfoByIno len:%v",
			mpId, len(deletedVerInfoByMpId.DirDeletedVerInfoByIno)) //TODO:tangjingyu del

		for _, deletedVerInfoByIno := range deletedVerInfoByMpId.DirDeletedVerInfoByIno {
			log.LogInfof("#### [GetDirDeletedVerInfoByMpIdPersist] mpId:%v, deletedVerInfoByIno:%+v",
				mpId, deletedVerInfoByIno) //TODO:tangjingyu del

			dirDeletedVerInfoByMpIdPersist := newDirDeletedVerInfoByMpIdPersist(mpId)
			p := newDirDeletedVerInfoByInoPersist(deletedVerInfoByIno)
			dirDeletedVerInfoByMpIdPersist.DeletedVerByInoList = append(dirDeletedVerInfoByMpIdPersist.DeletedVerByInoList, p)

			deletedDirVerInfoList = append(deletedDirVerInfoList, dirDeletedVerInfoByMpIdPersist)
			log.LogInfof("#### [GetDirDeletedVerInfoByMpIdPersist] appneded.... deletedDirVerInfoList: %+v", deletedDirVerInfoList)
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
			log.LogWarnf("[loadDirDelVerInfo] ToDelDirVersionInfoList idx[%v] is nil", idx)
			continue
		}

		log.LogDebugf("### [loadDirDelVerInfo] ToDelDirVersionInfoList idx[%v]: %#v", idx, toDel)
		dirVerMgr.AddDirToDelVerInfos(toDel.MpId, toDel.DirToDelVerInfoList, false)
	}

	//2. load deleted version info
	for _, deletedByMpId := range persist.DeletedDirVerInfoList {
		for _, deletedByIno := range deletedByMpId.DeletedVerByInoList {
			for _, deletedVer := range deletedByIno.DeletedVerList {
				dirVerMgr.AddDirDeletedVer(deletedByMpId.MpId, deletedByIno.DirInode, deletedByIno.SubRootIno, deletedVer)
			}
		}
	}

	log.LogInfof("action[loadDirDelVerInfo] vol[%v] load done, toDelListLen :%v, deletedListLen: %v",
		dirVerMgr.vol.Name, len(persist.ToDelDirVersionInfoList), len(persist.DeletedDirVerInfoList))
	return nil
}

func (dirVerMgr *DirSnapVersionManager) init(cluster *Cluster) error {
	log.LogWarnf("action[DirSnapVersionManager.init] vol %v", dirVerMgr.vol.Name)
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

func (dirVerMgr *DirSnapVersionManager) AddDirToDelVerInfos(mpId uint64, infoList []proto.DelDirVersionInfo, doPersist bool) (err error) {
	if _, err = dirVerMgr.vol.metaPartition(mpId); err != nil {
		log.LogErrorf("[AddDirToDelVerInfos] vol[%v] not exist mpId:%v", dirVerMgr.vol.Name, mpId)
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
	}

	for _, info := range infoList {
		if len(info.DelVers) == 0 {
			log.LogErrorf("[AddDirToDelVerInfos] len(DelDirVersionInfo.DelVers) is 0, dirInode:%v, mpId:%v, ",
				info.DirIno, mpId)
			continue
		}

		var dirToDelVerInfos *DirToDelVerInfosByIno
		if dirToDelVerInfos, ok = dirToDelVerInfosOfMp.ToDelVerInfoByInoMap[info.DirIno]; !ok {
			dirToDelVerInfos = newDirToDelVerInfos(info.DirIno, info.SubRootIno)
			dirToDelVerInfosOfMp.ToDelVerInfoByInoMap[info.DirIno] = dirToDelVerInfos
		}

		dirToDelVerInfos.AddDirToDelVers(info.DelVers)
		addCnt++
	}

	if addCnt > 0 {
		if doPersist {
			if err = dirVerMgr.PersistDirDelVerInfo(); err != nil {
				log.LogErrorf("[AddDirToDelVerInfos] PersistDirDelVerInfo failed, MpId:%v, err:%v", mpId, err.Error())
				return
			}
			log.LogInfof("[AddDirToDelVerInfos] vol[%v] mpId[%v] PersistDirDelVerInfo done, add count:%v",
				dirVerMgr.vol.Name, mpId, addCnt)
		} else {
			log.LogInfof("[AddDirToDelVerInfos] vol[%v] mpId[%v] add count:%v", dirVerMgr.vol.Name, mpId, addCnt)
		}
	} else {
		log.LogInfof("[AddDirToDelVerInfos] nothing changed, vol[%v] mpId[%v]", dirVerMgr.vol.Name, mpId)
	}

	return
}

type ToDelDirVersionInfo struct {
	VolName         string
	MetaPartitionId uint64
	DirInfos        []proto.DelDirVersionInfo //TODO: change to pointer
}

//TODO:tangjingyu return pointer
//for lcNode
func (dirVerMgr *DirSnapVersionManager) getToDelDirVersionInfoListWithLock() (toDelDirVersionInfoList []ToDelDirVersionInfo) {
	//TODO: lock scope
	dirVerMgr.DeVerInfoLock.Lock()
	defer dirVerMgr.DeVerInfoLock.Unlock()

	return dirVerMgr.getToDelDirVersionInfoList()
}

func (dirVerMgr *DirSnapVersionManager) getToDelDirVersionInfoList() (toDelDirVersionInfoList []ToDelDirVersionInfo) {
	toDelDirVersionInfoList = make([]ToDelDirVersionInfo, 0)

	for _, dirToDelVerInfoByMpId := range dirVerMgr.toDelDirVerByMpIdMap {
		toDelDirVersionInfo := ToDelDirVersionInfo{
			VolName:         dirVerMgr.vol.Name,
			MetaPartitionId: dirToDelVerInfoByMpId.MetaPartitionId,
			DirInfos:        make([]proto.DelDirVersionInfo, 0),
		}
		log.LogDebugf("#### [getToDelDirVersionInfoList] dirToDelVerInfoByMpId.ToDelVerInfoByInoMap len:%v",
			len(dirToDelVerInfoByMpId.ToDelVerInfoByInoMap)) //TODO:tangjignyu del

		for _, dirToDelVerInfos := range dirToDelVerInfoByMpId.ToDelVerInfoByInoMap {
			log.LogDebugf("#### [getToDelDirVersionInfoList] range ToDelVerInfoByInoMap, DirIno:%v", dirToDelVerInfos.DirInode) //TODO:tangjignyu del
			delDirVersionInfo := proto.DelDirVersionInfo{
				DirIno:     dirToDelVerInfos.DirInode,
				SubRootIno: dirToDelVerInfos.SubRootIno,
				DelVers:    make([]proto.DelVer, 0),
			}

			for verToDel := range dirToDelVerInfos.ToDelVerSet {
				log.LogDebugf("#### [getToDelDirVersionInfoList] range ToDelVerSet, verToDel:%v", verToDel) //TODO:tangjignyu del
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

	log.LogDebugf("[getToDelDirVersionInfoList] vol[%v], got item cnt:%v",
		dirVerMgr.vol.Name, len(toDelDirVersionInfoList))
	return toDelDirVersionInfoList
}

// RemoveDirToDelVer :
// caller must handle the lock properly
// called when lcNode actually deleted the dir version
func (dirVerMgr *DirSnapVersionManager) RemoveDirToDelVer(metaPartitionId, dirIno uint64, deletedVer uint64) (ret *DirToDelVerInfosByIno) {
	var dirToDelVerInfoByMpId *DirToDelVerInfoByMpId
	var dirToDelVerInfos *DirToDelVerInfosByIno
	var ok bool

	if dirToDelVerInfoByMpId, ok = dirVerMgr.toDelDirVerByMpIdMap[metaPartitionId]; !ok {
		log.LogErrorf("[RemoveDirToDelVer]: vol[%v] not exist DirToDelVerInfoByMpId record with metaPartitionId=%v",
			dirVerMgr.vol.Name, metaPartitionId)
		return
	}

	if dirToDelVerInfos, ok = dirToDelVerInfoByMpId.ToDelVerInfoByInoMap[dirIno]; !ok {
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
	ret = dirToDelVerInfos

	if len(dirToDelVerInfos.ToDelVerSet) == 0 {
		log.LogInfof("[RemoveDirToDelVer]: vol[%v] mpId[%v] dirInodeId[%v]: remove all versions to delete of dir, latest remove ver: %v",
			dirVerMgr.vol.Name, metaPartitionId, dirIno, deletedVer)
		delete(dirToDelVerInfoByMpId.ToDelVerInfoByInoMap, dirIno)

		if len(dirToDelVerInfoByMpId.ToDelVerInfoByInoMap) == 0 {
			log.LogInfof("[RemoveDirToDelVer]: vol[%v] mpId[%v]: remove all versions to delete of metaPartition",
				dirVerMgr.vol.Name, metaPartitionId)
			delete(dirVerMgr.toDelDirVerByMpIdMap, metaPartitionId)
		}
	}

	return
}

// AddDirDeletedVer :
// caller must handle the lock properly
// called when lcNode actually deleted the dir version
func (dirVerMgr *DirSnapVersionManager) AddDirDeletedVer(metaPartitionId, dirIno, subRootIno, deletedVer uint64) (changed bool) {
	var deletedVerInfoByIno *DirDeletedVerInfoByIno
	var deletedVerInfoByMpId *DirDeletedVerInfoByMpId
	var ok bool

	if deletedVerInfoByMpId, ok = dirVerMgr.deletedDirVerByMpIdMap[metaPartitionId]; !ok {
		log.LogDebugf("[AddDirDeletedVer]: vol[%v] has no record of mpId:%v", dirVerMgr.vol.Name, metaPartitionId)
		deletedVerInfoByMpId = newDirDeletedVerInfoByMpId(metaPartitionId)
		dirVerMgr.deletedDirVerByMpIdMap[metaPartitionId] = deletedVerInfoByMpId
	}

	if deletedVerInfoByIno, ok = deletedVerInfoByMpId.DirDeletedVerInfoByIno[dirIno]; !ok {
		log.LogDebugf("[AddDirDeletedVer]: vol[%v] mpId[%v] has no record of dirInodeId=%v",
			dirVerMgr.vol.Name, metaPartitionId, dirIno)
		deletedVerInfoByIno = newDirDeletedVerInfoByIno(dirIno, subRootIno)
		deletedVerInfoByMpId.DirDeletedVerInfoByIno[dirIno] = deletedVerInfoByIno
	}

	if _, ok = deletedVerInfoByIno.DeletedVerSet[deletedVer]; ok {
		log.LogInfof("[AddDirDeletedVer]: vol[%v] mpId[%v] dirInodeId[%v] already exists deletedVer: %v",
			dirVerMgr.vol.Name, metaPartitionId, dirIno, deletedVer)
		changed = false
	} else {
		log.LogInfof("[AddDirDeletedVer]: vol[%v] mpId[%v] dirInodeId[%v] add deletedVer: %v",
			dirVerMgr.vol.Name, metaPartitionId, dirIno, deletedVer)
		deletedVerInfoByIno.DeletedVerSet[deletedVer] = struct{}{}
		changed = true
	}
	return
}

func (dirVerMgr *DirSnapVersionManager) DelVer(metaPartitionId, dirIno, deletedVer uint64) (err error) {
	//TODO: lock scope
	dirVerMgr.DeVerInfoLock.Lock()
	defer dirVerMgr.DeVerInfoLock.Unlock()

	log.LogDebugf("[DelVer] vol[%v] mpId[%v]  dirIno[%v] del ver:%v",
		dirVerMgr.vol.Name, metaPartitionId, dirIno, deletedVer)

	ToDelVerInfosByIno := dirVerMgr.RemoveDirToDelVer(metaPartitionId, dirIno, deletedVer)
	if ToDelVerInfosByIno == nil {
		log.LogErrorf("[DelVer] vol[%v] mpId[%v]  dirIno[%v] no record of del ver:%v",
			dirVerMgr.vol.Name, metaPartitionId, dirIno, deletedVer)
		return
	}

	changed := dirVerMgr.AddDirDeletedVer(metaPartitionId, dirIno, ToDelVerInfosByIno.SubRootIno, deletedVer)
	if !changed {
		log.LogErrorf("[DelVer] vol[%v] mpId[%v]  dirIno[%v] already has record of deleted ver:%v",
			dirVerMgr.vol.Name, metaPartitionId, dirIno, deletedVer)
	}

	if ToDelVerInfosByIno != nil {
		err = dirVerMgr.PersistDirDelVerInfo()
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
		var deletedVerInfoByIno *DirDeletedVerInfoByIno
		if deletedVerInfoByIno, ok = deletedVerInfoByMpId.DirDeletedVerInfoByIno[verItem.DirSnapIno]; !ok {
			log.LogErrorf("[RemoveDirDeletedVer] vol[%v] mpId[%v] no record of dirInodeId:%v",
				dirVerMgr.vol.Name, mpId, verItem.DirSnapIno)
			continue
		}

		if _, ok := deletedVerInfoByIno.DeletedVerSet[verItem.Ver]; !ok {
			log.LogErrorf("[RemoveDirDeletedVer] vol[%v] mpId[%v] dirInodeId[%v] no record of del ver:%v",
				dirVerMgr.vol.Name, mpId, verItem.DirSnapIno, verItem.Ver)
			continue
		}
		delete(deletedVerInfoByIno.DeletedVerSet, verItem.Ver)
		changed = true

		if len(deletedVerInfoByIno.DeletedVerSet) == 0 {
			delete(deletedVerInfoByMpId.DirDeletedVerInfoByIno, verItem.DirSnapIno)
			log.LogDebugf("[RemoveDirDeletedVer] vol[%v] mpId[%v] clean record of dirInodeId:%v",
				dirVerMgr.vol.Name, mpId, verItem.DirSnapIno)

			if len(deletedVerInfoByMpId.DirDeletedVerInfoByIno) == 0 {
				delete(dirVerMgr.deletedDirVerByMpIdMap, mpId)
				log.LogDebugf("[RemoveDirDeletedVer] vol[%v] clean record of mpId:%v",
					dirVerMgr.vol.Name, mpId)
			}
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
	log.LogDebugf("[CheckDirDeletedVer] vol[%v] deletedDirVerByMpIdMap len:%v",
		dirVerMgr.vol.Name, len(dirVerMgr.deletedDirVerByMpIdMap))
	deletedDirVerByMpIdMapCopy := make(map[uint64]*DirDeletedVerInfoByMpId)

	//copy for preventing network requests from occupying the lock
	dirVerMgr.DeVerInfoLock.RLock()
	for mpId, deletedDirVerByMpId := range dirVerMgr.deletedDirVerByMpIdMap {
		deletedVerInfoByMpIdCopy := newDirDeletedVerInfoByMpId(mpId)

		for dirInoddId, deletedVerInfoByIno := range deletedDirVerByMpId.DirDeletedVerInfoByIno {
			deletedDirVerByInodeCopy := newDirDeletedVerInfoByIno(deletedVerInfoByIno.DirInode, deletedVerInfoByIno.SubRootIno)

			for delVer := range deletedVerInfoByIno.DeletedVerSet {
				deletedDirVerByInodeCopy.DeletedVerSet[delVer] = struct{}{}
			}

			deletedVerInfoByMpIdCopy.DirDeletedVerInfoByIno[dirInoddId] = deletedDirVerByInodeCopy
		}

		deletedDirVerByMpIdMapCopy[mpId] = deletedVerInfoByMpIdCopy
	}
	dirVerMgr.DeVerInfoLock.RUnlock()

	for mpId, deletedDirVerByMpId := range deletedDirVerByMpIdMapCopy {
		for _, deletedVerInfoByIno := range deletedDirVerByMpId.DirDeletedVerInfoByIno {
			deletedVerList := make([]proto.DirVerItem, 0)

			for deletedVer := range deletedVerInfoByIno.DeletedVerSet {
				dirVerItem := proto.DirVerItem{
					DirSnapIno: deletedVerInfoByIno.DirInode,
					RootIno:    deletedVerInfoByIno.SubRootIno,
					Ver:        deletedVer,
				}
				deletedVerList = append(deletedVerList, dirVerItem)
			}

			if err := dirVerMgr.ReqMetaNodeToBatchDelDirSnapVer(mpId, deletedVerList); err != nil {
				log.LogErrorf("[CheckDirDeletedVer] failed to create batch del task to metaNode, err:%v, vol:%v, mpId:%v",
					err.Error(), dirVerMgr.vol.Name, mpId)
				continue
			}

			log.LogInfof("[CheckDirDeletedVer] vol[%v] mpId[%v] batch del task to metaNode done, deletedVerList len: %v",
				dirVerMgr.vol.Name, mpId, len(deletedVerList))
		}
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
