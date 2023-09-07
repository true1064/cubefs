package impl

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/proto"
	"path"
	"strings"
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
		if e.SnapshotInode != dirIno {
			continue
		}

		for _, v := range e.Vers {
			if v.Ver.IsNormal() {
				names = append(names, versionName(v.OutVer))
			}
		}
		break
	}

	return names
}

func (m *snapMetaOpImp) versionExist(dirIno uint64, outVer string) (bool, *proto.VersionInfo) {
	for _, e := range m.snapShotItems {
		if e.SnapshotInode != dirIno {
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
	span := trace.SpanFromContext(ctx)
	if !strings.HasPrefix(name, sdk.SnapShotPre) {
		return false, nil
	}

	if m.hasSetVer && parentId != m.snapIno {
		span.Warnf("already set ver before, maybe conflict, parentId %d, name %s, ver %s",
			parentId, name, m.getVerStr())
		return false, sdk.ErrConflict
	}

	var ver *proto.DelVer

	buildVer := func(idx uint64, vers []*proto.ClientDirVer) {
		ver = &proto.DelVer{
			DelVer: idx,
		}

		for _, v := range vers {
			ver.Vers = append(ver.Vers, v.Ver)
		}
	}

	for _, e := range m.snapShotItems {
		if e.SnapshotInode != parentId {
			continue
		}

		for _, v := range e.Vers {
			vName := versionName(v.OutVer)
			if name == vName && v.Ver.Status == proto.VersionNormal {
				buildVer(v.Ver.Ver, e.Vers)
				ver.Vers = append(ver.Vers, &proto.VersionInfo{
					Ver:     e.MaxVer,
					DelTime: 0,
					Status:  proto.VersionInit,
				})
				break
			}
		}

		break
	}

	if ver == nil {
		return false, sdk.ErrNotFound
	}

	m.hasSetVer = true
	m.ver = ver
	m.isNewest = false
	m.snapIno = parentId
	m.sm.SetVerInfo(ver)
	return true, nil
}

func buildFromClientVers(maxVer uint64, clientVers []*proto.ClientDirVer) (vers []*proto.VersionInfo) {
	vers = make([]*proto.VersionInfo, 0)
	for _, cv := range clientVers {
		vers = append(vers, cv.Ver)
	}
	vers = append(vers, &proto.VersionInfo{
		Ver:     maxVer,
		DelTime: 0,
		Status:  proto.VersionInit,
	})
	return vers
}

func (m *snapMetaOpImp) checkSnapshotIno(dirIno uint64) {
	if m.hasSetVer {
		return
	}

	for _, e := range m.snapShotItems {
		if e.SnapshotInode != dirIno {
			continue
		}
		ver := &proto.DelVer{
			DelVer: e.MaxVer,
			Vers:   buildFromClientVers(e.MaxVer, e.Vers),
		}

		m.sm.SetVerInfo(ver)
		m.ver = ver
		m.isNewest = true
		m.hasSetVer = true
		m.snapIno = dirIno
		return
	}

	return

}

func (m *snapMetaOpImp) isSnapshotInode(dirIno uint64) (bool, *proto.DelVer) {
	for _, e := range m.snapShotItems {
		if e.SnapshotInode == dirIno {
			ver := &proto.DelVer{
				DelVer: e.MaxVer,
				Vers:   buildFromClientVers(e.MaxVer, e.Vers),
			}
			return true, ver
		}
	}

	return false, nil
}

func (m *snapMetaOpImp) newestVer() bool {
	if m.ver == nil {
		return true
	}

	return m.isNewest
}

func newDirDentry(dirIno uint64, name string) (den *proto.Dentry) {
	return &proto.Dentry{
		Name:   name,
		Inode:  dirIno,
		Type:   uint32(defaultDirMod),
		FileId: 0,
	}
}

func (m *snapMetaOpImp) LookupEx(ctx context.Context, parentId uint64, name string) (den *proto.Dentry, err error) {
	span := trace.SpanFromContextSafe(ctx)

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

	return m.sm.InodeCreate_ll(mode, 0, 0, nil, nil)
}

func (m *snapMetaOpImp) CreateFileEx(ctx context.Context, parentID uint64, name string, mode uint32) (*sdk.InodeInfo, error) {
	m.checkSnapshotIno(parentID)

	span := trace.SpanFromContextSafe(ctx)
	ifo, err := m.CreateInode(mode)
	if err != nil {
		span.Errorf("create inode failed, err %s", err.Error())
		return nil, err
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
		return nil, err
	}

	return sdk.NewInode(ifo, fileId), nil
}

func (m *snapMetaOpImp) CreateDentryEx(ctx context.Context, req *sdk.CreateDentryReq) (uint64, error) {
	span := trace.SpanFromContextSafe(ctx)
	m.checkSnapshotIno(req.ParentId)
	if !m.newestVer() {
		span.Warnf("can't write on snapshot dir, snap %v", m.getVerStr())
		return 0, sdk.ErrWriteSnapshot
	}
	if isSnapshotName(req.Name) {
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

func (m *snapMetaOpImp) LookupPath(subdir string) (uint64, error) {
	return 0, nil
}

func (m *snapMetaOpImp) ReadDirLimit(parentID uint64, from string, limit uint64) ([]proto.Dentry, error) {
	m.checkSnapshotIno(parentID)
	if !m.newestVer() || !m.hasSetVer {
		return m.sm.ReadDirLimit_ll(parentID, from, limit)
	}

	// snapshot inode, return version info
	items, err := m.sm.ReadDirLimit_ll(parentID, from, limit)
	if err != nil {
		return nil, err
	}

	// insert version info to items
	versionNames := m.getVersionNames(parentID)
	vItems := make([]proto.Dentry, 0, len(versionNames))
	for _, v := range versionNames {
		vItems = append(vItems, *newDirDentry(parentID, v))
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
	if err != nil {
		return err
	}

	dstParIno, dstName, dstVer := getDir(dst)
	if err != nil {
		return err
	}

	m.sm.SetRenameVerInfo(srcVer, dstVer)
	err = m.sm.Rename_ll(srcParIno, srcName, dstParIno, dstName, false)
	if err != nil {
		span.Errorf("rename failed, src %s, dst %s, srcIno %d, srcName %s, dstIno %d, dstName %s, err %s",
			src, dst, srcParIno, srcName, dstParIno, dstName, err.Error())
		return err
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

func (m *snapMetaOpImp) XAttrSet(inode uint64, name, value []byte) error {
	m.checkSnapshotIno(inode)
	if !m.newestVer() {
		return sdk.ErrWriteSnapshot
	}
	return m.sm.XAttrSet_ll(inode, name, value)
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

func (m *snapMetaOpImp) ListMultipart_ll(prefix, delimiter, keyMarker string, multipartIdMarker string, maxUploads uint64) (sessionResponse []*proto.MultipartInfo, err error) {
	return m.sm.ListMultipart_ll(prefix, delimiter, keyMarker, multipartIdMarker, maxUploads)
}

func (m *snapMetaOpImp) ListAllDirSnapshot(subRootIno uint64) ([]*proto.DirSnapshotInfo, error) {
	return m.sm.ListAllDirSnapshot(subRootIno)
}

func (m *snapMetaOpImp) conflict(ctx context.Context, filePath string) bool {
	span := trace.SpanFromContextSafe(ctx)
	for _, e := range m.snapShotItems {
		if e.SnapshotDir == filePath {
			continue
		}

		if strings.HasPrefix(e.SnapshotDir, filePath) || strings.HasPrefix(filePath, e.SnapshotDir) {
			span.Warnf("filePath %s is conflict with before snapshot dir %s", filePath, e.SnapshotDir)
			return true
		}
	}
	return false
}
