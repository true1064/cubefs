package master

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"sort"
	"sync"
	"time"
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
	DirInode      uint64
	SubRootIno    uint64
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

func (dirVerAlloc *DirSnapVerAllocator) load(val []byte) (err error) {
	persistVer := &DirSnapVerAllocatorPersist{}
	if err = json.Unmarshal(val, persistVer); err != nil {
		return
	}

	dirVerAlloc.PreAllocMaxVer = persistVer.PreAllocMaxVer
	dirVerAlloc.CurSnapVer = persistVer.PreAllocMaxVer
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
	toDelDirVerInfoMap map[uint64]*DirToDelVerInfoByMpId
	// key: metaPartitionId
	deletedDirVerInfoMap map[uint64]*DirDeletedVerInfoByIno
}

func newDirSnapVersionManager(vol *Vol) *DirSnapVersionManager {
	return &DirSnapVersionManager{
		vol: vol,

		dirVerAllocator:      newDirSnapVerAllocator(),
		toDelDirVerInfoMap:   make(map[uint64]*DirToDelVerInfoByMpId),
		deletedDirVerInfoMap: make(map[uint64]*DirDeletedVerInfoByIno),
	}
}

//TODO: del ver info
func (dirVerMgr *DirSnapVersionManager) String() string {
	return fmt.Sprintf("DirSnapVersionManager:{vol[%v], %v}",
		dirVerMgr.vol.Name, dirVerMgr.dirVerAllocator.String())
}

func (direrMgr *DirSnapVersionManager) SetCluster(c *Cluster) {
	direrMgr.c = c
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
		DeletedVerList: make([]uint64, len(dInfo.DeletedVerSet)),
	}

	for deletedVer := range dInfo.DeletedVerSet {
		dirDeletedVerInfoByInoPersist.DeletedVerList = append(dirDeletedVerInfoByInoPersist.DeletedVerList, deletedVer)
	}

	return dirDeletedVerInfoByInoPersist
}

type DirDeletedVerInfoByMpIdPersist struct {
	mpId                uint64
	deletedVerByInoList []*DirDeletedVerInfoByInoPersist
}

func newDirDeletedVerInfoByMpIdPersist(mpId uint64) *DirDeletedVerInfoByMpIdPersist {
	return &DirDeletedVerInfoByMpIdPersist{
		mpId:                mpId,
		deletedVerByInoList: make([]*DirDeletedVerInfoByInoPersist, 0),
	}
}

type DirDelVerInfoPersist struct {
	toDelDirVersionInfoList []*DirToDelVersionInfoByMpIdPersist
	deletedDirVerInfoList   []*DirDeletedVerInfoByMpIdPersist
}

func (direrMgr *DirSnapVersionManager) GetDirToDelVerInfoByMpIdPersist() []*DirToDelVersionInfoByMpIdPersist {
	toDelDirVersionInfoList := direrMgr.getToDelDirVersionInfoList()

	toDelDirVersionInfoListPersist := make([]*DirToDelVersionInfoByMpIdPersist, len(toDelDirVersionInfoList))

	for _, toDelVerInfo := range toDelDirVersionInfoList {
		toDelDirVersionInfoListPersist = append(toDelDirVersionInfoListPersist, newDirToDelVersionInfoByMpIdPersist(toDelVerInfo))
	}

	return toDelDirVersionInfoListPersist
}

func (direrMgr *DirSnapVersionManager) GetDirDeletedVerInfoByMpIdPersist() []*DirDeletedVerInfoByMpIdPersist {
	deletedDirVerInfoList := make([]*DirDeletedVerInfoByMpIdPersist, 0)

	for mpId, dirDeletedVerInfoByIno := range direrMgr.deletedDirVerInfoMap {
		dirDeletedVerInfoByMpIdPersist := newDirDeletedVerInfoByMpIdPersist(mpId)

		p := newDirDeletedVerInfoByInoPersist(dirDeletedVerInfoByIno)
		dirDeletedVerInfoByMpIdPersist.deletedVerByInoList = append(dirDeletedVerInfoByMpIdPersist.deletedVerByInoList, p)

		deletedDirVerInfoList = append(deletedDirVerInfoList, dirDeletedVerInfoByMpIdPersist)
	}

	return deletedDirVerInfoList
}

//PersistDirDelVerInfo :
// caller must handle the lock properly
func (direrMgr *DirSnapVersionManager) PersistDirDelVerInfo() (err error) {
	persist := &DirDelVerInfoPersist{
		toDelDirVersionInfoList: direrMgr.GetDirToDelVerInfoByMpIdPersist(),
		deletedDirVerInfoList:   direrMgr.GetDirDeletedVerInfoByMpIdPersist(),
	}

	var val []byte
	if val, err = json.Marshal(persist); err != nil {
		err = fmt.Errorf("[PersistDirDelVerInfo]: Marshal failed, vol: %v, err: %v", direrMgr.vol.Name, err)
		return
	}

	return direrMgr.c.syncDirDelVersionInfo(direrMgr.vol, val)
}

func (dirVerMgr *DirSnapVersionManager) loadDirVersionAllocator(val []byte) (err error) {
	return dirVerMgr.dirVerAllocator.load(val)
}

func (dirVerMgr *DirSnapVersionManager) loadDirDelVerInfo(val []byte) (err error) {
	persist := &DirDelVerInfoPersist{}
	if err = json.Unmarshal(val, persist); err != nil {
		return
	}

	//1. load to-del version info
	for _, toDel := range persist.toDelDirVersionInfoList {
		dirVerMgr.AddDirToDelVerInfos(toDel.MpId, toDel.DirToDelVerInfoList)
	}

	//2. load deleted version info
	for _, deletedByMpId := range persist.deletedDirVerInfoList {

		for _, deletedByIno := range deletedByMpId.deletedVerByInoList {

			for _, deletedVer := range deletedByIno.DeletedVerList {
				dirVerMgr.AddDirDeletedVer(deletedByMpId.mpId, deletedByIno.DirInode, deletedByIno.SubRootIno, deletedVer)
			}
		}
	}

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

//TODO:tangjingyu check if mpId exists
func (dirVerMgr *DirSnapVersionManager) AddDirToDelVerInfos(mpId uint64, infoList []proto.DelDirVersionInfo) (err error) {
	var changed bool

	dirVerMgr.DeVerInfoLock.Lock()
	defer dirVerMgr.DeVerInfoLock.Unlock()

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
		if err = dirVerMgr.PersistDirDelVerInfo(); err != nil {
			log.LogErrorf("[AddDirToDelVerInfos]: PersistDirDelVerInfo failed, mpId:%v, err:%v", mpId, err.Error())
			return
		}
	} else {
		log.LogInfof("[AddDirToDelVerInfos]: nothing changed, mpId:%v, ", mpId)
	}

	return
}

type ToDelDirVersionInfo struct {
	VolName         string
	MetaPartitionId uint64
	DirInfos        []proto.DelDirVersionInfo //TODO: change to pointer
}

func (dirVerMgr *DirSnapVersionManager) getToDelDirVersionInfoList() (toDelDirVersionInfoList []ToDelDirVersionInfo) {
	toDelDirVersionInfoList = make([]ToDelDirVersionInfo, 0)

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

//TODO:tangjingyu return pointer
//for lcNode
func (dirVerMgr *DirSnapVersionManager) getToDelDirVersionInfoListWithLock() (toDelDirVersionInfoList []ToDelDirVersionInfo) {
	//TODO: lock scope
	dirVerMgr.DeVerInfoLock.Lock()
	defer dirVerMgr.DeVerInfoLock.Unlock()

	return dirVerMgr.getToDelDirVersionInfoList()
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
func (dirVerMgr *DirSnapVersionManager) AddDirDeletedVer(metaPartitionId, dirIno, subRootIno, deletedVer uint64) (changed bool) {
	var deletedVerInfoByIno *DirDeletedVerInfoByIno
	var ok bool

	if deletedVerInfoByIno, ok = dirVerMgr.deletedDirVerInfoMap[metaPartitionId]; !ok {
		log.LogDebugf("[AddDirDeletedVer]: vol[%v] has no record with metaPartitionId=%v, dirInodeId=%v",
			dirVerMgr.vol.Name, metaPartitionId, dirIno)
		deletedVerInfoByIno = newDirDeletedVerInfos(dirIno, subRootIno)
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
	dirVerMgr.DeVerInfoLock.Lock()
	defer dirVerMgr.DeVerInfoLock.Unlock()

	removedToDelVer := dirVerMgr.RemoveDirToDelVer(metaPartitionId, dirIno, deletedVer)

	addedDeletedVer := dirVerMgr.AddDirDeletedVer(metaPartitionId, dirIno, 0, deletedVer) //TODO: ROOT INO

	if removedToDelVer || addedDeletedVer {
		err = dirVerMgr.PersistDirDelVerInfo()
	}

	return
}

func (dirVerMgr *DirSnapVersionManager) RemoveDirDeleteVer(mpId uint64) (err error) {
	dirVerMgr.DeVerInfoLock.Lock()
	defer dirVerMgr.DeVerInfoLock.Unlock()

	delete(dirVerMgr.deletedDirVerInfoMap, mpId)
	if err = dirVerMgr.PersistDirDelVerInfo(); err != nil {
		log.LogErrorf("[RemoveDirDeleteVer] PersistDirDelVerInfo failed, err:%v, vol:%v, mpId:%v",
			err.Error(), dirVerMgr.vol.Name, mpId)
	}
	return
}

//TODO:tangjingyu: recored on flight request and not req repeatlly
func (dirVerMgr *DirSnapVersionManager) ReqMetaNodeToBatchDelDirSnapVer(mpId uint64, deletedVers []proto.DirVerItem) (err error) {
	var (
		mp *MetaPartition
		mr *MetaReplica
	)

	if mp, err = dirVerMgr.c.getMetaPartitionByID(mpId); err != nil {
		log.LogErrorf("[ReqMetaNodeToBatchDelDirSnapVer] err:%v, vol:%v, mpId:%v",
			err.Error(), dirVerMgr.vol.Name, mpId)
		return
	}

	if mr, err = mp.getMetaReplicaLeader(); err != nil {
		log.LogErrorf("[ReqMetaNodeToBatchDelDirSnapVer] get MetaReplica leader fail, err:%v, vol:%v, mpId:%v",
			err.Error(), dirVerMgr.vol.Name, mpId)
		return
	}

	task := mr.metaNode.createDirVerDelTask(dirVerMgr.vol.Name, mpId, deletedVers)
	mr.metaNode.Sender.syncSendAdminTask(task)
	if _, err = mr.metaNode.Sender.syncSendAdminTask(task); err != nil {
		log.LogErrorf("[ReqMetaNodeToBatchDelDirSnapVer] req metaNode(%v) batch del dir ver failed, err:%v, vol:%v, mpId:%v",
			mr.Addr, err.Error(), dirVerMgr.vol.Name, mpId)
		return
	}

	if err = dirVerMgr.RemoveDirDeleteVer(mpId); err != nil {
		log.LogErrorf("[ReqMetaNodeToBatchDelDirSnapVer] RemoveDirDeleteVer failed, err:%v, vol:%v, mpId:%v",
			mr.Addr, err.Error(), dirVerMgr.vol.Name, mpId)
		return
	}

	return
}

func (dirVerMgr *DirSnapVersionManager) CheckDirDeletedVer() {
	dirVerMgr.DeVerInfoLock.RLock()
	deletedDirVerInfoMapCopy := make(map[uint64]*DirDeletedVerInfoByIno)
	for mpId, deletedVerInfoByIno := range dirVerMgr.deletedDirVerInfoMap {
		deletedDirVerInfoMapCopy[mpId] = deletedVerInfoByIno
	}
	dirVerMgr.DeVerInfoLock.RUnlock()

	for mpId, deletedVerInfoByIno := range deletedDirVerInfoMapCopy {
		deletedVerList := make([]proto.DirVerItem, len(deletedVerInfoByIno.DeletedVerSet))

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
		}
		log.LogDebugf("[CheckDirDeletedVer] create batch del task to metaNode, vol:%v, mpId:%v",
			dirVerMgr.vol.Name, mpId)
	}

	return
}
