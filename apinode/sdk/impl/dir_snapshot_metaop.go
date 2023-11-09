package impl

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/proto"
)

type snapMetaOpImp struct {
	sm            MetaOp
	allocId       func(ctx context.Context) (id uint64, err error)
	snapShotItems []*proto.DirSnapshotInfo
	hasSetVer     bool
	ver           *proto.DelVer
	snapIno       uint64
	isNewest      bool
	rootIno       uint64
}

func newSnapMetaOp(mop MetaOp, items []*proto.DirSnapshotInfo, rootIno uint64) *snapMetaOpImp {
	nmw, ok := mop.(*metaOpImp)
	sm := mop
	if ok {
		sm = nmw.SnapShotMetaWrapper.Clone()
	}

	smw := &snapMetaOpImp{
		sm:            sm,
		snapShotItems: items,
		rootIno:       rootIno,
		isNewest:      true,
	}
	return smw
}

func (m *snapMetaOpImp) getVerStr() string {
	if m.ver == nil {
		return "0-no version"
	}

	return m.ver.String()
}

func versionName(ver string) string {
	return fmt.Sprintf("%s%s", sdk.SnapShotPre, ver)
}

func isSnapshotName(name string) bool {
	return strings.HasPrefix(name, sdk.SnapShotPre)
}

func (m *snapMetaOpImp) getVersionNames(dirIno uint64) (names []string) {
	names = make([]string, 0)

	for _, e := range m.snapShotItems {
		if !e.IsSnapshotInode(dirIno) {
			continue
		}

		for _, v := range e.Vers {
			if v.Normal() {
				names = append(names, versionName(v.OutVer))
			}
		}
		break
	}

	return names
}

func (m *snapMetaOpImp) versionExist(dirIno uint64, outVer string) (bool, *proto.VersionInfo) {
	for _, e := range m.snapShotItems {
		if !e.IsSnapshotInode(dirIno) {
			continue
		}

		for _, v := range e.Vers {
			if v.OutVer == outVer {
				return true, v.Ver
			}
		}
		break
	}
	return false, nil
}

func (m *snapMetaOpImp) isSnapshotDir(ctx context.Context, parentId uint64, name string) (bool, error) {
	if !strings.HasPrefix(name, sdk.SnapShotPre) {
		return false, nil
	}

	span := trace.SpanFromContext(ctx)
	var ver *proto.DelVer
	for _, e := range m.snapShotItems {
		if !e.IsSnapshotInode(parentId) {
			continue
		}

		for _, v := range e.Vers {
			vName := versionName(v.OutVer)
			if name == vName && v.Normal() {
				ver = &proto.DelVer{
					DelVer: v.Ver.Ver,
					Vers:   buildByClientVerItems(e.Vers),
				}
				break
			}
		}

		break
	}

	if ver == nil {
		span.Warnf("not found version info, parIno %d, name %s", parentId, name)
		return false, sdk.ErrNotFound
	}

	m.hasSetVer = true
	m.ver = ver
	m.isNewest = false
	m.sm.SetVerInfoEx(ver, parentId)
	return true, nil
}

func buildByClientVerItems(clientVerItems []*proto.ClientDirVer) (items []*proto.VersionInfo) {
	items = make([]*proto.VersionInfo, 0, len(clientVerItems))
	for _, cv := range clientVerItems {
		if cv.Ver.Status == proto.VersionDeleting || cv.Ver.Status == proto.VersionMarkDelete {
			continue
		}
		items = append(items, cv.Ver)
	}
	return items
}

// resetDirVer used by multipart to ensure only when complete op is relative to snapshot.
func (m *snapMetaOpImp) resetDirVer(ctx context.Context) {
	if !m.hasSetVer {
		return
	}

	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("reset dir ver info, ver %s", m.ver.String())
	m.hasSetVer = false
	m.sm.SetVerInfoEx(nil, 0)
}

func (m *snapMetaOpImp) checkSnapshotIno(dirIno uint64) {
	if m.hasSetVer {
		return
	}

	isSnapshot, ver := m.isSnapshotInode(dirIno)
	if !isSnapshot {
		return
	}

	m.sm.SetVerInfoEx(ver, dirIno)
	m.ver = ver
	m.isNewest = true
	m.hasSetVer = true
}

func (m *snapMetaOpImp) isSnapshotInode(dirIno uint64) (bool, *proto.DelVer) {
	for _, e := range m.snapShotItems {
		if e.IsSnapshotInode(dirIno) {
			ver := &proto.DelVer{
				DelVer: e.MaxVer,
				Vers:   buildByClientVerItems(e.Vers),
			}
			return true, ver
		}
	}

	return false, nil
}

func (m *snapMetaOpImp) newestVer() bool {
	if !m.hasSetVer {
		return true
	}

	return m.isNewest
}

func newDirDentry(dirIno uint64, name string) (den *proto.Dentry) {
	return &proto.Dentry{
		Name:   name,
		Inode:  dirIno,
		Type:   uint32(defaultDirMod),
		FileId: sdk.SnapShotFileId,
	}
}

func (m *snapMetaOpImp) LookupEx(ctx context.Context, parentId uint64, name string) (den *proto.Dentry, err error) {
	span := trace.SpanFromContextSafe(ctx)
	m.checkSnapshotIno(parentId)

	isSnapShot, err := m.isSnapshotDir(ctx, parentId, name)
	if err != nil {
		span.Warnf("check snapshot dir failed, parentId %d, name %s, err %s", parentId, name, err.Error())
		return
	}

	if isSnapShot {
		span.Debugf("parentId %d name %s is a snapshot dir", parentId, name)
		return newDirDentry(parentId, name), nil
	}

	return m.sm.LookupEx_ll(parentId, name)
}

func (m *snapMetaOpImp) CreateInode(mode uint32) (*proto.InodeInfo, error) {
	if !m.newestVer() {
		return nil, sdk.ErrWriteSnapshot
	}

	return m.sm.InodeCreate_ll(0, mode, 0, 0, nil, nil)
}

func (m *snapMetaOpImp) CreateFileEx(ctx context.Context, parentID uint64, name string, mode uint32) (*sdk.InodeInfo, uint64, error) {
	m.checkSnapshotIno(parentID)

	span := trace.SpanFromContextSafe(ctx)
	ifo, err := m.CreateInode(mode)
	if err != nil {
		span.Errorf("create inode failed, err %s", err.Error())
		return nil, 0, err
	}
	span.Debugf("create inode success, %v", ifo.String())

	req := &sdk.CreateDentryReq{
		ParentId: parentID,
		Name:     name,
		Inode:    ifo.Inode,
		OldIno:   0,
		Mode:     mode,
	}

	fileId, err := m.CreateDentryEx(ctx, req)
	if err != nil {
		span.Errorf("create dentry failed, req %v, err %s", req, err.Error())
		return nil, 0, err
	}

	return ifo, fileId, nil
}

func (m *snapMetaOpImp) CreateDentryEx(ctx context.Context, req *sdk.CreateDentryReq) (uint64, error) {
	span := trace.SpanFromContextSafe(ctx)
	m.checkSnapshotIno(req.ParentId)
	if !m.newestVer() {
		span.Warnf("can't write on snapshot dir, snap %s", m.getVerStr())
		return 0, sdk.ErrWriteSnapshot
	}
	if isSnapshotName(req.Name) {
		span.Warnf("request name is not valid, name %s", req.Name)
		return 0, sdk.ErrSnapshotName
	}

	fileId, err := m.allocId(ctx)
	if err != nil {
		span.Errorf("alloc id failed, err %s", err.Error())
		return 0, err
	}

	createReq := &proto.CreateDentryRequest{
		ParentID: req.ParentId,
		Name:     req.Name,
		Inode:    req.Inode,
		OldIno:   req.OldIno,
		Mode:     req.Mode,
		FileId:   fileId,
	}

	err = m.sm.DentryCreateEx_ll(createReq)
	if err != nil {
		span.Errorf("create dentry failed, req %v, err %s", req, err.Error())
		return 0, err
	}

	return fileId, nil
}

func (m *snapMetaOpImp) Delete(parentID uint64, name string, isDir bool) (*proto.InodeInfo, error) {
	m.checkSnapshotIno(parentID)
	if isSnapshotName(name) || !m.newestVer() {
		return nil, sdk.ErrWriteSnapshot
	}

	return m.sm.Delete_ll(parentID, name, isDir)
}

func (m *snapMetaOpImp) Truncate(inode, size uint64) error {
	if !m.newestVer() {
		return sdk.ErrWriteSnapshot
	}

	return m.sm.Truncate(inode, size)
}

func (m *snapMetaOpImp) InodeUnlink(inode uint64) (*proto.InodeInfo, error) {
	m.checkSnapshotIno(inode)
	if !m.newestVer() {
		return nil, sdk.ErrWriteSnapshot
	}

	return m.sm.InodeUnlink_ll(inode)
}

func (m *snapMetaOpImp) Evict(inode uint64) error {
	m.checkSnapshotIno(inode)
	if !m.newestVer() {
		return sdk.ErrWriteSnapshot
	}

	return m.sm.Evict(inode)
}

func (m *snapMetaOpImp) Setattr(inode uint64, valid, mode, uid, gid uint32, atime, mtime int64) error {
	m.checkSnapshotIno(inode)
	if !m.newestVer() {
		return sdk.ErrWriteSnapshot
	}

	return m.sm.Setattr(inode, valid, mode, uid, gid, atime, mtime)
}

func (m *snapMetaOpImp) InodeDelete(inode uint64) error {
	m.checkSnapshotIno(inode)
	if !m.newestVer() {
		return sdk.ErrWriteSnapshot
	}
	return m.sm.InodeDelete_ll(inode)
}

// InodeDeleteVer used to delete inode generated by multipart upload.
func (m *snapMetaOpImp) InodeDeleteVer(inode uint64) error {
	return m.sm.InodeDelete_ll(inode)
}

func (m *snapMetaOpImp) ReadDirLimit(dirIno uint64, from string, limit uint64) ([]proto.Dentry, error) {
	m.checkSnapshotIno(dirIno)
	if !m.newestVer() || !m.hasSetVer {
		return m.sm.ReadDirLimit_ll(dirIno, from, limit)
	}

	// snapshot inode, return version info
	items, err := m.sm.ReadDirLimit_ll(dirIno, from, limit)
	if err != nil {
		return nil, err
	}

	// insert version info to items
	versionNames := m.getVersionNames(dirIno)
	vItems := make([]proto.Dentry, 0, len(versionNames))
	for _, v := range versionNames {
		vItems = append(vItems, *newDirDentry(dirIno, v))
	}

	cnt := int(limit)
	result := make([]proto.Dentry, 0, cnt)
	if len(items) == 0 {
		result = vItems
	}

	for idx, e := range items {
		if strings.Compare(e.Name, sdk.SnapShotPre) < 0 {
			result = append(result, e)
			continue
		}

		for _, v := range vItems {
			result = append(result, v)
		}
		result = append(result, items[idx:]...)
		break
	}

	for idx, d := range result {
		if strings.Compare(d.Name, from) <= 0 {
			continue
		}

		if idx+cnt > len(result) {
			return result[idx:], nil
		}

		return result[idx : idx+cnt], nil
	}

	return []proto.Dentry{}, nil
}

func (m *snapMetaOpImp) Rename(ctx context.Context, src, dst string) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("start rename, src %s, dst %s", src, dst)
	if src == "" || dst == "" {
		return sdk.ErrBadRequest
	}

	if strings.Contains(src, sdk.SnapShotPre) || strings.Contains(dst, sdk.SnapShotPre) {
		return sdk.ErrWriteSnapshot
	}

	src = "/" + src
	dst = "/" + dst

	getDir := func(subPath string) (parIno uint64, name string, ver *proto.DelVer) {
		dir, name := path.Split(subPath)
		parIno, ver, err = m.lookupSubDirVer(m.rootIno, dir)
		if err != nil {
			span.Warnf("lookup path failed, rootIno %d, dir %s, err %s", m.rootIno, dir, err.Error())
		}
		return parIno, name, ver
	}

	srcParIno, srcName, srcVer := getDir(src)
	dstParIno, dstName, dstVer := getDir(dst)

	m.sm.SetRenameVerInfo(srcVer, dstVer)
	err = m.sm.Rename_ll(srcParIno, srcName, dstParIno, dstName, false)
	if err != nil {
		span.Errorf("rename failed, src %s, dst %s, srcIno %d, srcName %s, dstIno %d, dstName %s, err %s",
			src, dst, srcParIno, srcName, dstParIno, dstName, err.Error())
		return err
	}

	return nil
}


const (
	snapshotCntOneDir = 10
	snapshotCntOneUser = 100
)

func (m *snapMetaOpImp) checkSnapshotCntLimit(parIno uint64) error {
	if m.snapShotItems == nil {
		return nil
	}

	total := 0
	for _, item := range m.snapShotItems {
		cnt := 0
		for _, v := range item.Vers {
			if v.Ver.IsNormal() {
				cnt ++
			}
		}

		// not calc latest version
		cnt --
		if item.SnapshotInode == parIno && cnt >= snapshotCntOneDir {
			return fmt.Errorf("parIno %d snapshot cnt is already over 10, now %d", parIno, cnt)
		}
		total += cnt
	}

	if total >= snapshotCntOneUser {
		return fmt.Errorf("total snapshot cnt is already over 100, now %d", total)
	}
	return nil
}

func (m *snapMetaOpImp) lookupSubDirVer(parIno uint64, subPath string) (childIno uint64, v *proto.DelVer, err error) {

	getVer := func(ino uint64) {
		ok, ver := m.isSnapshotInode(ino)
		if ok {
			v = ver
		}
	}

	getVer(parIno)

	names := strings.Split(subPath, "/")
	childIno = parIno
	for _, name := range names {
		if name == "" {
			continue
		}

		den, err1 := m.sm.LookupEx_ll(parIno, name)
		if err1 != nil {
			err = err1
			return
		}

		if !proto.IsDir(den.Type) {
			err = sdk.ErrNotDir
			return
		}

		childIno = den.Inode
		parIno = childIno
		getVer(childIno)
	}

	return
}

func (m *snapMetaOpImp) AppendExtentKeys(inode uint64, eks []proto.ExtentKey) error {
	return m.sm.AppendExtentKeys(inode, eks)
}

func (m *snapMetaOpImp) BatchSetXAttr(inode uint64, attrs map[string]string) error {
	m.checkSnapshotIno(inode)
	if !m.newestVer() {
		return sdk.ErrWriteSnapshot
	}

	return m.sm.BatchSetXAttr_ll(inode, attrs)
}

func (m *snapMetaOpImp) XAttrGetAll(inode uint64) (*proto.XAttrInfo, error) {
	m.checkSnapshotIno(inode)
	return m.sm.XAttrGetAll_ll(inode)
}

func (m *snapMetaOpImp) SetInodeLock(inode uint64, req *proto.InodeLockReq) error {
	m.checkSnapshotIno(inode)
	if !m.newestVer() {
		return sdk.ErrWriteSnapshot
	}

	return m.sm.SetInodeLock_ll(inode, req)
}

func (m *snapMetaOpImp) InodeGet(inode uint64) (*proto.InodeInfo, error) {
	m.checkSnapshotIno(inode)
	return m.sm.InodeGet_ll(inode)
}

func (m *snapMetaOpImp) XAttrSet(inode uint64, name, value []byte, overwrite bool) error {
	m.checkSnapshotIno(inode)
	if !m.newestVer() {
		return sdk.ErrWriteSnapshot
	}
	return m.sm.XAttrSetEx_ll(inode, name, value, overwrite)
}

func (m *snapMetaOpImp) XAttrGet_ll(inode uint64, name string) (*proto.XAttrInfo, error) {
	m.checkSnapshotIno(inode)
	return m.sm.XAttrGet_ll(inode, name)
}

func (m *snapMetaOpImp) XAttrDel_ll(inode uint64, name string) error {
	m.checkSnapshotIno(inode)
	if !m.newestVer() {
		return sdk.ErrWriteSnapshot
	}
	return m.sm.XAttrDel_ll(inode, name)
}

func (m *snapMetaOpImp) XBatchDelAttr_ll(ino uint64, keys []string) error {
	m.checkSnapshotIno(ino)
	if !m.newestVer() {
		return sdk.ErrWriteSnapshot
	}
	return m.sm.XBatchDelAttr_ll(ino, keys)
}

func (m *snapMetaOpImp) XAttrsList_ll(inode uint64) ([]string, error) {
	m.checkSnapshotIno(inode)
	if !m.newestVer() {
		return nil, sdk.ErrWriteSnapshot
	}
	return m.sm.XAttrsList_ll(inode)
}

func (m *snapMetaOpImp) InitMultipart_ll(path string, extend map[string]string) (multipartId string, err error) {
	if !m.newestVer() {
		return "", sdk.ErrWriteSnapshot
	}
	if strings.Contains(path, sdk.SnapShotPre) {
		return "", sdk.ErrWriteSnapshot
	}
	return m.sm.InitMultipart_ll(path, extend)
}

func (m *snapMetaOpImp) GetMultipart_ll(path, multipartId string) (info *proto.MultipartInfo, err error) {
	return m.sm.GetMultipart_ll(path, multipartId)
}

func (m *snapMetaOpImp) AddMultipartPart_ll(path, multipartId string, partId uint16, size uint64, md5 string, inodeInfo *proto.InodeInfo) (oldInode uint64, updated bool, err error) {
	if !m.newestVer() {
		return 0, false, sdk.ErrWriteSnapshot
	}
	return m.sm.AddMultipartPart_ll(path, multipartId, partId, size, md5, inodeInfo)
}

func (m *snapMetaOpImp) RemoveMultipart_ll(path, multipartID string) (err error) {
	if !m.newestVer() {
		return sdk.ErrWriteSnapshot
	}
	return m.sm.RemoveMultipart_ll(path, multipartID)
}

//func (m *snapMetaOpImp) ListMultipart_ll(prefix, delimiter, keyMarker string, multipartIdMarker string, maxUploads uint64) (sessionResponse []*proto.MultipartInfo, err error) {
//	return m.sm.ListMultipart_ll(prefix, delimiter, keyMarker, multipartIdMarker, maxUploads)
//}

func (m *snapMetaOpImp) ListAllDirSnapshot(subRootIno uint64) ([]*proto.DirSnapshotInfo, error) {
	return m.sm.ListAllDirSnapshot(subRootIno)
}

func (m *snapMetaOpImp) getSnapshotInodes() []uint64 {
	inodes := make([]uint64, 0, len(m.snapShotItems))
	for _, e := range m.snapShotItems {
		inodes = append(inodes, e.SnapshotInode)
	}
	return inodes
}
