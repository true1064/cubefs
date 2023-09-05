package main

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	blog "github.com/cubefs/cubefs/blobstore/util/log"

	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/apinode/sdk/impl"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/proto"
)

const (
	cluster = "cfs_dev"
	// cluster = "cfs_fault_test"
	addr = "172.16.1.101:17010,172.16.1.102:17010,172.16.1.103:17010"
	// addr = "10.177.111.194:17010"
	vol = "abc"
	// vol = "create-vol-test"
)

func main() {
	// log.InitFileLog("/tmp/cfs", "test", "debug")
	logDir := "/tmp/cfs/sdktest"
	log.InitLog(logDir, "test", log.DebugLevel, nil)
	stat.DefaultStatInterval = 2 * time.Second
	stat.NewStatistic(logDir, "test", int64(stat.DefaultStatLogSize), stat.DefaultTimeOutUs, true)
	mgr := impl.NewClusterMgr()
	blog.SetOutputLevel(blog.Ldebug)
	span, ctx := trace.StartSpanFromContext(context.TODO(), "")
	err := mgr.AddCluster(ctx, cluster, addr)
	if err != nil {
		span.Fatalf("init cluster failed, err %s", err.Error())
	}
	span.Infof("init cluster success")
	cluster := mgr.GetCluster(cluster)
	if cluster == nil {
		span.Fatalf("get cluster is nil")
	}

	vol := cluster.GetVol(vol)
	if vol == nil {
		span.Fatalf("vol is nil")
	}

	testDirOp(ctx, vol)
	testCreateFile(ctx, vol)
	testXAttrOp(ctx, vol)
	testMultiPartOp(ctx, vol)
	testInodeLock(ctx, vol)
}

// mkdir, readdir, deleteDir, createFile, deleteFile
func testDirOp(ctx context.Context, vol sdk.IVolume) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start test dir op ===================")
	defer span.Infof("end test dir op ===================")

	tmpDir := "testDirD6" + tmpString()
	dirIfo, _, err := vol.Mkdir(ctx, proto.RootIno, tmpDir)
	if err != nil {
		span.Fatalf("create dir failed, dir %s, err %s", tmpDir, err.Error())
	}

	span.Infof("create dir success, info %v", dirIfo)

	defer func() {
		err = vol.Delete(ctx, proto.RootIno, tmpDir, true)
		if err != nil {
			span.Fatalf("delete dir failed, dir %s err %s", tmpDir, err.Error())
		}
		span.Infof("delete dir success, dir %s", tmpDir)
	}()

	cases := []struct {
		dir  bool
		name string
		idx  int
	}{
		{false, "a1", 1},
		{false, "f1", 3},
		{true, "d2", 2},
		{false, "f2", 4},
		{false, "test0003", 7},
		{false, "test0001", 5},
		{false, "test0002", 6},
	}

	inos := make([]uint64, 0)
	var tmpInfo *sdk.InodeInfo
	for _, c := range cases {
		if c.dir {
			tmpInfo, _, err = vol.Mkdir(ctx, dirIfo.Inode, c.name)
		} else {
			tmpInfo, _, err = vol.CreateFile(ctx, dirIfo.Inode, c.name)
		}

		inos = append(inos, tmpInfo.Inode)

		if err != nil {
			span.Fatalf("mkdir sub dir failed, name %s, err %s", c.name, err.Error())
		}

		span.Infof("mkdir sub file success, name %s, isDir %v, info %v", c.name, c.dir, tmpInfo)
	}

	defer func() {
		for _, c := range cases {
			err = vol.Delete(ctx, dirIfo.Inode, c.name, c.dir)
			if err != nil {
				span.Fatalf("delete sub file failed, name %s, dir %v, err %s", c.name, c.dir, err.Error())
			}
			span.Infof("delete dir success, name %s", c.name)
		}
	}()

	// readdirAll
	var items []sdk.DirInfo
	items, err = vol.ReadDirAll(ctx, dirIfo.Inode)
	if err != nil {
		span.Fatalf("read dir failed, ino %d, err %s", dirIfo.Inode, err.Error())
	}

	span.Infof("read dir success, get dents %d", len(items))

	for _, t := range cases {
		c := items[t.idx-1]
		if t.name != c.Name {
			span.Fatalf("read file order is not valid, get %s, want %s", c.Name, t.name)
		}
	}

	marker := ""
	totalItems := make([]sdk.DirInfo, 0)
	var tmpItems []sdk.DirInfo
	for {
		tmpItems, err = vol.Readdir(ctx, dirIfo.Inode, marker, 2)
		if err != nil {
			span.Fatalf("readdir failed, ino %d, err %s", dirIfo.Inode, err.Error())
		}
		span.Infof("read limit, marker %v, items %v, total %d", marker, tmpItems, len(totalItems))
		if len(tmpItems) <= 1 {
			totalItems = append(totalItems, tmpItems[0])
			break
		}
		totalItems = append(totalItems, tmpItems[0])
		marker = tmpItems[1].Name
	}

	for _, t := range cases {
		c := totalItems[t.idx-1]
		if t.name != c.Name {
			span.Fatalf("read file order is not valid, get %s, want %s", c.Name, t.name)
		}
	}

	var inoInfos []*proto.InodeInfo
	inoInfos, err = vol.BatchGetInodes(ctx, inos)
	if err != nil {
		span.Fatalf("execute BatchGetInodes failed, err %s", err.Error())
	}

	for idx, ifo := range inoInfos {
		if ifo.Inode != inos[idx] {
			span.Fatalf("execute inoGet failed, got %d, want %d", ifo.Inode, inos[idx])
		}
	}
}

func tmpString() string {
	return fmt.Sprintf("tmp_%s", time.Now().String())
}

// test create, write, read, delete
func testCreateFile(ctx context.Context, vol sdk.IVolume) {
	span := trace.SpanFromContextSafe(ctx)
	tmpFile := "file" + tmpString()

	span.Info("start testCreateFile =================")
	defer span.Info("end testCreateFile =================")

	tmpDir := "dir" + tmpString()
	dirIfo, _, err := vol.Mkdir(ctx, proto.RootIno, tmpDir)
	if err != nil {
		span.Fatalf("create dir failed, dir %s, err %s", tmpDir, err.Error())
	}

	span.Infof("create dir success, info %v", dirIfo)

	defer func() {
		err = vol.Delete(ctx, proto.RootIno, tmpDir, true)
		if err != nil {
			span.Fatalf("delete dir failed, dir %s err %s", tmpDir, err.Error())
		}
		span.Infof("delete dir success, dir %s", tmpDir)
	}()

	var tmpInfo *sdk.InodeInfo
	tmpInfo, _, err = vol.CreateFile(ctx, dirIfo.Inode, tmpFile)
	if err != nil {
		span.Fatalf("create file failed, name %s", tmpFile)
	}
	span.Infof("create file success, ifo %v", tmpInfo)

	var lookInfo *sdk.DirInfo
	lookInfo, err = vol.Lookup(ctx, dirIfo.Inode, tmpFile)
	if err != nil {
		span.Fatalf("execute look up failed, name %s, err %s", tmpFile, err.Error())
	}
	if lookInfo.Inode != tmpInfo.Inode {
		span.Fatalf("execute lookup result not valid, want %d, got %d", tmpInfo.Inode, lookInfo.Inode)
	}

	if lookInfo.FileId == 0 {
		span.Fatalf("crate dentry fileId can't be zero, info %v", lookInfo)
	}

	var getInfo *proto.InodeInfo
	getInfo, err = vol.GetInode(ctx, tmpInfo.Inode)
	if err != nil {
		span.Fatalf("execute get Inode failed, ino %d, err %s", tmpInfo.Inode, err.Error())
	}

	if getInfo.Inode != tmpInfo.Inode || getInfo.Mode != tmpInfo.Mode || getInfo.ModifyTime != tmpInfo.ModifyTime {
		span.Fatalf("get inode is valid, get %v, want %v", getInfo, tmpInfo)
	}

	defer func() {
		err = vol.Delete(ctx, dirIfo.Inode, tmpFile, false)
		if err != nil {
			span.Fatalf("delete file failed, file %s, err %s", tmpFile, err.Error())
		}
		span.Infof("delete file success, file %s", tmpFile)
	}()

	data := []byte("testxtadaadadaada")
	size := len(data)
	err = vol.WriteFile(ctx, tmpInfo.Inode, 0, uint64(size), bytes.NewBuffer(data))
	if err != nil {
		span.Fatalf("write file failed, ino %d, err %s", tmpInfo.Inode, err.Error())
	}

	out := make([]byte, 1024)
	readN := 0
	readN, err = vol.ReadFile(ctx, tmpInfo.Inode, 0, out)
	if err != nil {
		span.Fatalf("read file failed, ino %d, err %s", tmpInfo.Inode, err.Error())
	}

	if readN != size {
		span.Fatalf("read file size error, got %d, want %s", readN, size)
	}

	if string(data) != string(out[:readN]) {
		span.Fatalf("read file data not equal to input")
	}

	// test file upload
	req := &sdk.UploadFileReq{
		ParIno:    dirIfo.Inode,
		Name:      "file2",
		OldFileId: 0,
		Extend:    map[string]string{"k1": "v1"},
		Body:      bytes.NewBuffer(data),
	}

	var uploadIfo *sdk.InodeInfo
	uploadIfo, _, err = vol.UploadFile(ctx, req)
	if err != nil {
		span.Fatalf("upload file failed, name %s, err %s", req.Name, err.Error())
	}
	span.Infof("upload file success, info %v", uploadIfo)

	den, err := vol.Lookup(ctx, req.ParIno, req.Name)
	if err != nil {
		span.Fatalf("look up path failed, err %s, name %s", err.Error(), req.Name)
	}

	req.OldFileId = den.FileId
	req.Body = bytes.NewBuffer(data)
	uploadIfo, _, err = vol.UploadFile(ctx, req)
	if err != nil {
		span.Fatalf("upload file failed, name %s, err %s", req.Name, err.Error())
	}

	newName := "testNewName" + tmpString()
	err = vol.Rename(ctx, dirIfo.Inode, dirIfo.Inode, req.Name, newName)
	if err != nil {
		span.Fatalf("rename file failed, err %s", err.Error())
	}

	// test rename dest already exist, should be failed
	tmpName := "test" + tmpString()
	_, _, err = vol.CreateFile(ctx, dirIfo.Inode, tmpName)
	if err != nil {
		span.Fatalf("create file failed, err %s", err.Error())
	}

	err = vol.Rename(ctx, dirIfo.Inode, dirIfo.Inode, newName, tmpName)
	if err != sdk.ErrExist {
		span.Fatalf("if target file exist, should be failed, err %v", err)
	}

	defer func() {
		err = vol.Delete(ctx, dirIfo.Inode, newName, false)
		if err != nil {
			span.Fatalf("delete file failed, file %s, err %s", newName, err.Error())
		}
		err = vol.Delete(ctx, dirIfo.Inode, tmpName, false)
		if err != nil {
			span.Fatalf("delete file failed, file %s, err %s", tmpName, err.Error())
		}
	}()

	var val string
	val, err = vol.GetXAttr(ctx, uploadIfo.Inode, "k1")
	if err != nil {
		span.Fatalf("execute xAttr failed, ino %d, err %s", uploadIfo.Inode, err.Error())
	}

	if val != "v1" {
		span.Fatalf("getXAttr result error, got %v", val)
	}

	var st *sdk.StatFs
	st, err = vol.StatFs(ctx, dirIfo.Inode)
	if err != nil {
		span.Fatalf("stat dir failed, ino %d, err %s", dirIfo.Inode, err.Error())
	}

	if st.Size != size*2 {
		span.Fatalf("stat fs get result not valid, got %d, want %d", st.Size, size*2)
	}

	mtime := time.Now().Unix() + 10
	attrReq := &sdk.SetAttrReq{
		Ino:   uploadIfo.Inode,
		Flag:  proto.AttrModifyTime,
		Mode:  0,
		Uid:   0,
		Gid:   0,
		Atime: 0,
		Mtime: uint64(mtime),
	}
	err = vol.SetAttr(ctx, attrReq)
	if err != nil {
		span.Fatalf("set attr failed, req %v, err %s", attrReq, err.Error())
	}

	var newUploadIfo *proto.InodeInfo
	newUploadIfo, err = vol.GetInode(ctx, uploadIfo.Inode)
	if err != nil {
		span.Fatalf("get inode failed, ino %d, err %s", uploadIfo.Inode, err.Error())
	}

	if newUploadIfo.ModifyTime.Unix() != mtime {
		span.Fatalf("get ino time exception, got %d, want %d", newUploadIfo.ModifyTime.Unix(), mtime)
	}
}

func testXAttrOp(ctx context.Context, vol sdk.IVolume) {
	span := trace.SpanFromContextSafe(ctx)
	tmpFile := "testXAttrOp"

	span.Info("start testXAttrOp =================")
	defer span.Info("end testXAttrOp =================")

	inoIfo, _, err := vol.CreateFile(ctx, proto.RootIno, tmpFile)
	if err != nil {
		span.Fatalf("create file failed, name %s, err %s", tmpFile, err.Error())
	}
	span.Infof("create file success, result %v", inoIfo)

	defer func() {
		err = vol.Delete(ctx, proto.RootIno, tmpFile, false)
		if err != nil {
			span.Fatalf("delete file failed, file %s, err %s", tmpFile, err.Error())
		}
	}()

	ino := inoIfo.Inode
	key := "k1"
	val := "v1"
	err = vol.SetXAttr(ctx, ino, key, val)
	if err != nil {
		span.Fatalf("setXAttr failed, ino %d, err %s", ino, err.Error())
	}

	err = vol.SetXAttrNX(ctx, ino, key, val)
	if err != sdk.ErrExist {
		span.Fatalf("setXAttr failed, ino %d, err %v", ino, err)
	}

	var newVal string
	newVal, err = vol.GetXAttr(ctx, ino, key)
	if err != nil {
		span.Fatalf("getXAttr failed, ino %d, err %s", ino, err.Error())
	}

	if val != newVal {
		span.Fatalf("getXAttr failed, got %v, want %v", newVal, val)
	}

	err = vol.DeleteXAttr(ctx, ino, key)
	if err != nil {
		span.Fatalf("deleteXAttr failed, ino %d, err %s", ino, err.Error())
	}

	attrMap := map[string]string{}
	size := 10
	for idx := 0; idx < size; idx++ {
		tmpKey := fmt.Sprintf("key-%d", idx)
		tmpVal := fmt.Sprintf("val-%d", idx)
		attrMap[tmpKey] = tmpVal
	}

	err = vol.BatchSetXAttr(ctx, ino, attrMap)
	if err != nil {
		span.Fatalf("batch setXAttr failed, ino %d, err %s", ino, err.Error())
	}

	var keys []string
	keys, err = vol.ListXAttr(ctx, ino)
	if err != nil {
		span.Fatalf("list xAttr failed, ino %d, err %s", ino, err.Error())
	}

	if len(keys) != len(attrMap) {
		span.Fatalf("listXAttr failed, got %d, want %d", len(keys), len(attrMap))
	}

	var newAttrMap map[string]string
	newAttrMap, err = vol.GetXAttrMap(ctx, ino)
	if err != nil {
		span.Fatalf("getXAttr map failed, ino %d, err %s", ino, err.Error())
	}

	if len(newAttrMap) != size {
		span.Fatalf("getXAttr result not valid, ino %d, err %s", ino, err.Error())
	}

	for k, v := range newAttrMap {
		v1, ok := attrMap[k]
		if !ok || v1 != v {
			span.Fatalf("getXAttr map failed, key %s, want %s, got %s", k, v, v1)
		}
	}

	err = vol.BatchDeleteXAttr(ctx, ino, keys)
	if err != nil {
		span.Fatalf("execute batchDelete xAttr failed, ino %d, err %s", ino, err.Error())
	}

	var newKeys []string
	newKeys, err = vol.ListXAttr(ctx, ino)
	if err != nil {
		span.Fatalf("execute listXAttr failed, ino %d, err %s", ino, err.Error())
	}

	if len(newKeys) != 0 {
		span.Fatalf("execute batchDeleteXAttr failed, ino %d, err %s", ino, err.Error())
	}
}

func testMultiPartOp(ctx context.Context, vol sdk.IVolume) {
	span := trace.SpanFromContextSafe(ctx)
	tmpFile := "/testMultiPartOp10" + tmpString()

	span.Info("start testMultiPartOp =================")
	defer span.Info("end testMultiPartOp =================")

	uploadId, err := vol.InitMultiPart(ctx, tmpFile, nil)
	if err != nil {
		span.Fatalf("init multiPart failed, file %s, err %s", tmpFile, err.Error())
	}

	parts := []struct {
		num  uint16
		data string
	}{
		{1, "hello world"},
		{2, "hello test"},
		{3, "hello body"},
	}

	size := 0
	for _, p := range parts {
		_, err = vol.UploadMultiPart(ctx, tmpFile, uploadId, p.num, bytes.NewBufferString(p.data))
		if err != nil {
			span.Fatalf("upload multipart failed, num %d, err %s", p.num, err.Error())
		}
		size += len([]byte(p.data))
	}

	err = vol.AbortMultiPart(ctx, tmpFile, uploadId)
	if err != nil {
		span.Fatalf("abort multipart failed, file %s, id %s, err %s", tmpFile, uploadId, err.Error())
	}

	ext := map[string]string{"k1": "v1", "k2": "v2"}
	uploadId, err = vol.InitMultiPart(ctx, tmpFile, ext)
	if err != nil {
		span.Fatalf("init multiPart failed, file %s, err %s", tmpFile, err.Error())
	}

	for _, p := range parts {
		_, err = vol.UploadMultiPart(ctx, tmpFile, uploadId, p.num, bytes.NewBufferString(p.data))
		if err != nil {
			span.Fatalf("upload multipart failed, num %d, err %s", p.num, err.Error())
		}
	}

	partArr, next, isTrun, err := vol.ListMultiPart(ctx, tmpFile, uploadId, 10, 0)
	if err != nil {
		span.Fatalf("list multipart failed, err %s", err.Error())
	}

	if next != 0 || isTrun || len(parts) != len(partArr) {
		span.Fatalf("list multipart failed, next %d, trunc %v, arrLen(%d)", next, isTrun, len(parts))
	}

	newPartArr := make([]sdk.Part, 0)
	for _, part := range partArr {
		newPartArr = append(newPartArr, sdk.Part{
			ID:  part.ID,
			MD5: part.MD5,
		})
	}

	var finalIno *sdk.InodeInfo
	finalIno, _, err = vol.CompleteMultiPart(ctx, tmpFile, uploadId, 0, newPartArr)
	if err != nil {
		span.Fatalf("complete multipart failed, file %s, err %s", tmpFile, err.Error())
	}

	if finalIno.Size != uint64(size) {
		span.Fatalf("complete multipart failed, want %d, got %d", size, finalIno.Size)
	}

	newMap, err := vol.GetXAttrMap(ctx, finalIno.Inode)
	if err != nil {
		span.Fatalf("get xAttr map failed, ino %d, err %s", finalIno.Inode, err.Error())
	}

	if len(newMap) != len(ext) {
		span.Fatalf("get xAttr result not right, want %v, got %v", ext, newMap)
	}

	// test complete 1w part
	newTmpFile := tmpFile + tmpString()
	newUploadId, err := vol.InitMultiPart(ctx, newTmpFile, nil)
	if err != nil {
		span.Fatalf("init multiPart failed, file %s, err %s", newTmpFile, err.Error())
	}

	type pt struct {
		num  int
		data string
	}
	cnt := 1000
	newParts := make([]pt, 0, cnt)
	for idx := 0; idx < cnt; idx++ {
		newParts = append(newParts, pt{num: idx + 1, data: fmt.Sprintf("tmpData_%d", idx)})
	}

	respParts := make([]sdk.Part, 0, len(newParts))
	for _, p := range newParts {
		rp, nerr := vol.UploadMultiPart(ctx, newTmpFile, newUploadId, uint16(p.num), bytes.NewBufferString(p.data))
		if nerr != nil {
			span.Fatalf("upload multipart failed, num %d, err %s", p.num, nerr.Error())
		}
		size += len([]byte(p.data))
		respParts = append(respParts, *rp)
	}

	start := time.Now()
	_, _, err = vol.CompleteMultiPart(ctx, newTmpFile, newUploadId, 0, respParts)
	if err != nil {
		span.Fatalf("complete multipart failed, file %s, err %s", newTmpFile, err.Error())
	}
	span.Infof("complete multipart success, cnt %d, cost %s", len(respParts), time.Since(start).String())

	err = vol.Delete(ctx, proto.RootIno, strings.TrimPrefix(newTmpFile, "/"), false)
	if err != nil {
		span.Fatalf("delete multipart failed, file %v, err %s", newTmpFile, err.Error())
	}

	err = vol.Delete(ctx, proto.RootIno, strings.TrimPrefix(tmpFile, "/"), false)
	if err != nil {
		span.Fatalf("delete multipart failed, file %v, err %s", tmpFile, err.Error())
	}

	newTmp2File := tmpFile + tmpString()
	_, newName := path.Split(newTmp2File)
	req := &sdk.UploadFileReq{
		ParIno: proto.RootIno,
		Name:   newName,
		Body:   bytes.NewBufferString("hello world"),
	}
	_, oldId, err := vol.UploadFile(ctx, req)
	if err != nil {
		span.Fatalf("upload file failed, err %s, req %v", err.Error(), req)
	}

	newUploadId2, err := vol.InitMultiPart(ctx, newTmp2File, nil)
	if err != nil {
		span.Fatalf("init multiPart failed, file %s, err %s", tmpFile, err.Error())
	}

	for _, p := range parts {
		_, err = vol.UploadMultiPart(ctx, newTmp2File, newUploadId2, p.num, bytes.NewBufferString(p.data))
		if err != nil {
			span.Fatalf("upload multipart failed, num %d, err %s", p.num, err.Error())
		}
	}

	newPartArr2 := make([]sdk.Part, 0)
	for _, part := range partArr {
		newPartArr2 = append(newPartArr2, sdk.Part{
			ID:  part.ID,
			MD5: part.MD5,
		})
	}

	_, _, err = vol.CompleteMultiPart(ctx, newTmp2File, newUploadId2, oldId, newPartArr2)
	if err != nil {
		span.Fatalf("complete multipart failed, file %s, err %s", tmpFile, err.Error())
	}
}

func testInodeLock(ctx context.Context, vol sdk.IVolume) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start testInodeLock op ===================")
	defer span.Infof("end testInodeLock op ===================")

	dirName := "testInodeLock1"
	_ = vol.Delete(ctx, proto.RootIno, dirName, true)

	ifo, _, err := vol.Mkdir(ctx, proto.RootIno, dirName)
	if err != nil {
		span.Fatalf("create dir failed, dir %s, err %s", dirName, err.Error())
	}
	defer func() {
		err = vol.Delete(ctx, proto.RootIno, dirName, true)
		if err != nil {
			span.Fatalf("delete dir failed, dir %s, err %s", dirName, err.Error())
		}
	}()

	span.Infof("create ino success, ifo %v", ifo)

	ino := ifo.Inode
	lock := vol.NewInodeLock()
	err = lock.Lock(ctx, ino, int(time.Second*2))
	if err != nil {
		span.Fatalf("execute ino lock failed, ino %d, err %s", ifo.Inode, err.Error())
	}

	lock2 := vol.NewInodeLock()
	err = lock2.Lock(ctx, ino, int(time.Second*2))
	if err != sdk.ErrConflict {
		span.Fatalf("ino lock again should be failed, ino %d, err %v", ifo.Inode, err)
	}

	err = lock.UnLock(ctx, ino)
	if err != nil {
		span.Fatalf("execute ino unlock failed, ino %d, err %s", ifo.Inode, err.Error())
	}

	err = lock.UnLock(ctx, ino)
	if err == nil {
		span.Fatalf("execute ino unlock should fail, ino %d, err %v", ifo.Inode, err)
	}
}
