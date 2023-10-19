// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package stream

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"

	"golang.org/x/time/rate"
)

type SplitExtentKeyFunc func(parentInode, inode uint64, key proto.ExtentKey) error
type AppendExtentKeyFunc func(parentInode, inode uint64, key proto.ExtentKey, discard []proto.ExtentKey) error
type GetExtentsFunc func(inode uint64) (uint64, uint64, []proto.ExtentKey, error)
type TruncateFunc func(inode, size uint64) error
type EvictIcacheFunc func(inode uint64)
type LoadBcacheFunc func(key string, buf []byte, offset uint64, size uint32) (int, error)
type CacheBcacheFunc func(key string, buf []byte) error
type EvictBacheFunc func(key string) error

const (
	MaxMountRetryLimit = 6
	MountRetryInterval = time.Second * 5

	defaultReadLimitRate  = rate.Inf
	defaultReadLimitBurst = 128

	defaultWriteLimitRate  = rate.Inf
	defaultWriteLimitBurst = 128

	defaultStreamerLimit = 100000
	defMaxStreamerLimit  = 10000000
	kHighWatermarkPct    = 1.01
	slowStreamerEvictNum = 10
	fastStreamerEvictNum = 10000
)

var (
	// global object pools for memory optimization
	openRequestPool    *sync.Pool
	writeRequestPool   *sync.Pool
	flushRequestPool   *sync.Pool
	releaseRequestPool *sync.Pool
	truncRequestPool   *sync.Pool
	evictRequestPool   *sync.Pool
)

func init() {
	// init object pools
	openRequestPool = &sync.Pool{New: func() interface{} {
		return &OpenRequest{}
	}}
	writeRequestPool = &sync.Pool{New: func() interface{} {
		return &WriteRequest{}
	}}
	flushRequestPool = &sync.Pool{New: func() interface{} {
		return &FlushRequest{}
	}}
	releaseRequestPool = &sync.Pool{New: func() interface{} {
		return &ReleaseRequest{}
	}}
	truncRequestPool = &sync.Pool{New: func() interface{} {
		return &TruncRequest{}
	}}
	evictRequestPool = &sync.Pool{New: func() interface{} {
		return &EvictRequest{}
	}}
}

type ExtentConfig struct {
	Volume            string
	VolumeType        int
	Masters           []string
	FollowerRead      bool
	NearRead          bool
	Preload           bool
	ReadRate          int64
	WriteRate         int64
	BcacheEnable      bool
	BcacheDir         string
	MaxStreamerLimit  int64
	VerReadSeq        uint64
	OnAppendExtentKey AppendExtentKeyFunc
	OnSplitExtentKey  SplitExtentKeyFunc
	OnGetExtents      GetExtentsFunc
	OnTruncate        TruncateFunc
	OnEvictIcache     EvictIcacheFunc
	OnLoadBcache      LoadBcacheFunc
	OnCacheBcache     CacheBcacheFunc
	OnEvictBcache     EvictBacheFunc

	DisableMetaCache             bool
	MinWriteAbleDataPartitionCnt int
}

type MultiVerMgr struct {
	verReadSeq   uint64 // verSeq in config used as snapshot read
	latestVerSeq uint64 // newest verSeq from master for datanode write to check
	verList      *proto.VolVersionInfoList
	sync.RWMutex
}

// ExtentClient defines the struct of the extent client.
type ExtentClient struct {
	*ExtentClientVer
}

func (client *ExtentClientVer) UidIsLimited(uid uint32) bool {
	client.dataWrapper.UidLock.RLock()
	defer client.dataWrapper.UidLock.RUnlock()
	if uInfo, ok := client.dataWrapper.Uids[uid]; ok {
		if uInfo.Limited {
			log.LogDebugf("uid %v is limited", uid)
			return true
		}
	}
	log.LogDebugf("uid %v is not limited", uid)
	return false
}

func (client *extentClient) evictStreamer() bool {
	// remove from list
	item := client.streamerList.Back()
	if item == nil {
		return false
	}

	client.streamerList.Remove(item)
	ino := item.Value.(uint64)

	s, ok := client.streamers[ino]
	if !ok {
		return true
	}

	if s.isOpen {
		client.streamerList.PushFront(ino)
		return true
	}

	delete(s.client.streamers, s.inode)
	return true
}

func (client *extentClient) batchEvictStramer(batchCnt int) {
	client.streamerLock.Lock()
	defer client.streamerLock.Unlock()

	for cnt := 0; cnt < batchCnt; cnt++ {
		ok := client.evictStreamer()
		if !ok {
			break
		}
	}

}

func (client *extentClient) backgroundEvictStream() {
	t := time.NewTicker(2 * time.Second)
	for range t.C {
		start := time.Now()
		streamerSize := client.streamerList.Len()
		highWatermark := int(float32(client.maxStreamerLimit) * kHighWatermarkPct)
		for streamerSize > client.maxStreamerLimit {
			// fast evict
			if streamerSize > highWatermark {
				client.batchEvictStramer(fastStreamerEvictNum)
			} else {
				client.batchEvictStramer(slowStreamerEvictNum)
			}
			streamerSize = client.streamerList.Len()
			log.LogInfof("batch evict cnt(%d), cost(%d), now(%d)", 1, time.Since(start).Microseconds(), streamerSize)
		}
		log.LogInfof("streamer total cnt(%d), cost(%d) ns", streamerSize, time.Since(start).Nanoseconds())
	}
}

// NewExtentClient returns a new extent client.
func NewExtentClient(config *ExtentConfig) (client *ExtentClient, err error) {
	clientVer, err := NewExtentClientVer(config)
	if err != nil {
		return nil, err
	}

	client = &ExtentClient{
		ExtentClientVer: clientVer,
	}
	return client, nil
}

func (client *ExtentClientVer) GetEnablePosixAcl() bool {
	return client.dataWrapper.EnablePosixAcl
}

func (client *extentClient) GetFlowInfo() (*proto.ClientReportLimitInfo, bool) {
	log.LogInfof("action[ExtentClient.GetFlowInfo]")
	return client.LimitManager.GetFlowInfo()
}

func (client *extentClient) UpdateFlowInfo(limit *proto.LimitRsp2Client) {
	log.LogInfof("action[UpdateFlowInfo.UpdateFlowInfo]")
	client.LimitManager.SetClientLimit(limit)
	return
}

func (client *extentClient) SetClientID(id uint64) (err error) {
	client.LimitManager.ID = id
	return
}

func (client *ExtentClientVer) GetVolumeName() string {
	return client.volumeName
}

func (client *extentClient) GetLatestVer() uint64 {
	return atomic.LoadUint64(&client.multiVerMgr.latestVerSeq)
}
func (client *extentClient) GetReadVer() uint64 {
	return atomic.LoadUint64(&client.multiVerMgr.verReadSeq)
}
func (client *extentClient) GetVerMgr() *proto.VolVersionInfoList {
	return client.multiVerMgr.verList
}
func (client *extentClient) UpdateLatestVer(verList *proto.VolVersionInfoList) (err error) {
	verSeq := verList.GetLastVer()
	if verSeq == 0 || verSeq <= atomic.LoadUint64(&client.multiVerMgr.latestVerSeq) {
		return
	}
	client.multiVerMgr.Lock()
	defer client.multiVerMgr.Unlock()
	if verSeq <= atomic.LoadUint64(&client.multiVerMgr.latestVerSeq) {
		return
	}

	log.LogInfof("action[UpdateLatestVer] update verseq [%v] to [%v]", client.multiVerMgr.latestVerSeq, verSeq)
	atomic.StoreUint64(&client.multiVerMgr.latestVerSeq, verSeq)
	client.multiVerMgr.verList = verList

	client.streamerLock.Lock()
	defer client.streamerLock.Unlock()
	for _, streamer := range client.streamers {
		if streamer.verSeq != verSeq && !streamer.isDirVer {
			log.LogDebugf("action[ExtentClient.UpdateLatestVer] stream inode %v ver %v try update to %v", streamer.inode, streamer.verSeq, verSeq)

			streamer.verSeq = verSeq
			streamer.extents.verSeq = verSeq
			atomic.StoreInt32(&streamer.needUpdateVer, 1)
			log.LogDebugf("action[ExtentClient.UpdateLatestVer] finhsed stream inode %v ver update to %v", streamer.inode, verSeq)
		}
	}
	return nil
}

// Open request shall grab the lock until request is sent to the request channel
func (client *ExtentClientVer) OpenStream(inode uint64) error {
	client.streamerLock.Lock()
	s, ok := client.streamers[inode]
	if !ok {
		s = NewStreamer(client, inode)
		client.streamers[inode] = s
	}
	return s.IssueOpenRequest()
}

func (client *ExtentClientVer) OpenStreamVer(inode, seq uint64) error {
	client.streamerLock.Lock()
	s, ok := client.streamers[inode]
	if !ok {
		s = NewStreamer(client, inode)
		client.streamers[inode] = s
	}
	s.isDirVer = true
	s.verSeq = seq
	return s.IssueOpenRequest()
}

// Open request shall grab the lock until request is sent to the request channel
func (client *ExtentClientVer) OpenStreamWithCache(inode uint64, needBCache bool) error {
	client.streamerLock.Lock()
	s, ok := client.streamers[inode]
	if !ok {
		s = NewStreamer(client, inode)
		client.streamers[inode] = s
		if !client.disableMetaCache && needBCache {
			client.streamerList.PushFront(inode)
		}
	}
	s.needBCache = needBCache
	if !s.isOpen && !client.disableMetaCache {
		s.isOpen = true
		log.LogDebugf("open stream again, ino(%v)", s.inode)
		s.request = make(chan interface{}, 64)
		s.pendingCache = make(chan bcacheKey, 1)
		go s.server()
		go s.asyncBlockCache()
	}
	return s.IssueOpenRequest()
}

// Release request shall grab the lock until request is sent to the request channel
func (client *ExtentClientVer) CloseStream(inode uint64) error {
	client.streamerLock.Lock()
	s, ok := client.streamers[inode]
	if !ok {
		client.streamerLock.Unlock()
		return nil
	}
	return s.IssueReleaseRequest()
}

// Evict request shall grab the lock until request is sent to the request channel
func (client *ExtentClientVer) EvictStream(inode uint64) error {
	client.streamerLock.Lock()
	s, ok := client.streamers[inode]
	if !ok {
		client.streamerLock.Unlock()
		return nil
	}
	if s.isOpen {
		s.isOpen = false
		err := s.IssueEvictRequest()
		if err != nil {
			return err
		}
		s.done <- struct{}{}
	} else {
		delete(s.client.streamers, s.inode)
		s.client.streamerLock.Unlock()
	}

	return nil
}

// RefreshExtentsCache refreshes the extent cache.
func (client *ExtentClientVer) RefreshExtentsCache(inode uint64) error {
	s := client.GetStreamer(inode)
	if s == nil {
		return nil
	}
	return s.GetExtents()
}

func (client *ExtentClientVer) ForceRefreshExtentsCache(inode uint64) error {
	s := client.GetStreamer(inode)
	if s == nil {
		return nil
	}
	return s.GetExtentsForce()
}

// GetExtentCacheGen return extent generation
func (client *ExtentClientVer) GetExtentCacheGen(inode uint64) uint64 {
	s := client.GetStreamer(inode)
	if s == nil {
		return 0
	}
	return s.extents.gen
}

func (client *ExtentClientVer) GetExtents(inode uint64) []*proto.ExtentKey {
	s := client.GetStreamer(inode)
	if s == nil {
		return nil
	}
	return s.extents.List()
}

// FileSize returns the file size.
func (client *ExtentClientVer) FileSize(inode uint64) (size int, gen uint64, valid bool) {
	s := client.GetStreamer(inode)
	if s == nil {
		return
	}
	valid = true
	size, gen = s.extents.Size()
	return
}

// SetFileSize set the file size.
func (client *ExtentClientVer) SetFileSize(inode uint64, size int) {
	s := client.GetStreamer(inode)
	if s != nil {
		log.LogDebugf("SetFileSize: ino(%v) size(%v)", inode, size)
		s.extents.SetSize(uint64(size), true)
	}
}

// Write writes the data.
func (client *ExtentClientVer) Write(inode uint64, offset int, data []byte, flags int, checkFunc func() error) (write int, err error) {
	prefix := fmt.Sprintf("Write{ino(%v)offset(%v)size(%v)}", inode, offset, len(data))
	s := client.GetStreamer(inode)
	if s == nil {
		log.LogErrorf("Prefix(%v): stream is not opened yet", prefix)
		return 0, syscall.EBADF
	}

	s.once.Do(func() {
		// TODO unhandled error
		s.GetExtents()
	})

	write, err = s.IssueWriteRequest(offset, data, flags, checkFunc)
	if err != nil {
		log.LogError(errors.Stack(err))
		exporter.Warning(err.Error())
	}
	return
}

func (client *ExtentClientVer) Truncate(mw *meta.MetaWrapper, parentIno uint64, inode uint64, size int) error {
	prefix := fmt.Sprintf("Truncate{ino(%v)size(%v)}", inode, size)
	s := client.GetStreamer(inode)
	if s == nil {
		log.LogErrorf("Prefix(%v): stream is not opened yet", prefix)
		return syscall.EBADF
	}
	var info *proto.InodeInfo
	var err error
	var oldSize uint64
	if mw.EnableSummary {
		info, err = mw.InodeGet_ll(inode)
		oldSize = info.Size
	}
	err = s.IssueTruncRequest(size)
	if err != nil {
		err = errors.Trace(err, prefix)
		log.LogError(errors.Stack(err))
	}
	if mw.EnableSummary {
		go mw.UpdateSummary_ll(parentIno, 0, 0, int64(size)-int64(oldSize))
	}

	return err
}

func (client *ExtentClientVer) Flush(inode uint64) error {
	s := client.GetStreamer(inode)
	if s == nil {
		log.LogErrorf("Flush: stream is not opened yet, ino(%v)", inode)
		return syscall.EBADF
	}
	return s.IssueFlushRequest()
}

func (client *ExtentClientVer) Read(inode uint64, data []byte, offset int, size int) (read int, err error) {
	//log.LogErrorf("======> ExtentClient Read Enter, inode(%v), len(data)=(%v), offset(%v), size(%v).", inode, len(data), offset, size)
	//t1 := time.Now()
	if size == 0 {
		return
	}

	s := client.GetStreamer(inode)
	if s == nil {
		log.LogErrorf("Read: stream is not opened yet, ino(%v) offset(%v) size(%v)", inode, offset, size)
		return 0, syscall.EBADF
	}

	s.once.Do(func() {
		s.GetExtents()
	})

	err = s.IssueFlushRequest()
	if err != nil {
		return
	}

	read, err = s.read(data, offset, size)
	// log.LogErrorf("======> ExtentClient Read Exit, inode(%v), time[%v us].", inode, time.Since(t1).Microseconds())
	return
}

func (client *ExtentClientVer) ReadExtent(inode uint64, ek *proto.ExtentKey, data []byte, offset int, size int) (read int, err error, isStream bool) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("read-extent", err, bgTime, 1)
	}()

	var reader *ExtentReader
	var req *ExtentRequest
	if size == 0 {
		return
	}

	s := client.GetStreamer(inode)
	if s == nil {
		err = fmt.Errorf("Read: stream is not opened yet, ino(%v) ek(%v)", inode, ek)
		return
	}
	err = s.IssueFlushRequest()
	if err != nil {
		return
	}
	reader, err = s.GetExtentReader(ek)
	if err != nil {
		return
	}

	var needCache = false
	cacheKey := util.GenerateKey(s.client.volumeName, s.inode, ek.FileOffset)
	if _, ok := client.inflightL1cache.Load(cacheKey); !ok && client.shouldBcache() {
		client.inflightL1cache.Store(cacheKey, true)
		needCache = true
	}
	defer client.inflightL1cache.Delete(cacheKey)

	// do cache.
	if needCache {
		//read full extent
		buf := make([]byte, ek.Size)
		req = NewExtentRequest(int(ek.FileOffset), int(ek.Size), buf, ek)
		read, err = reader.Read(req)
		if err != nil {
			return
		}
		read = copy(data, req.Data[offset:offset+size])
		if client.cacheBcache != nil {
			buf := make([]byte, len(req.Data))
			copy(buf, req.Data)
			go func() {
				log.LogDebugf("ReadExtent L2->L1 Enter cacheKey(%v),client.shouldBcache(%v),needCache(%v)", cacheKey, client.shouldBcache(), needCache)
				if err := client.cacheBcache(cacheKey, buf); err != nil {
					client.BcacheHealth = false
					log.LogDebugf("ReadExtent L2->L1 failed, err(%v), set BcacheHealth to false.", err)
				}
				log.LogDebugf("ReadExtent L2->L1 Exit cacheKey(%v),client.BcacheHealth(%v),needCache(%v)", cacheKey, client.BcacheHealth, needCache)
			}()
		}
		return
	} else {
		//read data by offset:size
		req = NewExtentRequest(int(ek.FileOffset)+offset, size, data, ek)
		ctx := context.Background()
		s.client.readLimiter.Wait(ctx)
		s.client.LimitManager.ReadAlloc(ctx, size)
		isStream = true

		read, err = reader.Read(req)
		if err != nil {
			return
		}
		read = copy(data, req.Data)
		return
	}
}

// GetStreamer returns the streamer.
func (client *ExtentClientVer) GetStreamer(inode uint64) *Streamer {
	client.streamerLock.Lock()
	defer client.streamerLock.Unlock()
	s, ok := client.streamers[inode]
	if !ok {
		return nil
	}
	if !s.isOpen {
		s.isOpen = true
		s.request = make(chan interface{}, 64)
		s.pendingCache = make(chan bcacheKey, 1)
		go s.server()
		go s.asyncBlockCache()
	}
	return s
}

func (client *ExtentClientVer) GetRate() string {
	return fmt.Sprintf("read: %v\nwrite: %v\n", getRate(client.readLimiter), getRate(client.writeLimiter))
}

func (client *ExtentClientVer) shouldBcache() bool {
	return client.bcacheEnable && client.BcacheHealth
}

func getRate(lim *rate.Limiter) string {
	val := int(lim.Limit())
	if val > 0 {
		return fmt.Sprintf("%v", val)
	}
	return "unlimited"
}

func (client *ExtentClientVer) SetReadRate(val int) string {
	return setRate(client.readLimiter, val)
}

func (client *ExtentClientVer) SetWriteRate(val int) string {
	return setRate(client.writeLimiter, val)
}

func setRate(lim *rate.Limiter, val int) string {
	if val > 0 {
		lim.SetLimit(rate.Limit(val))
		return fmt.Sprintf("%v", val)
	}
	lim.SetLimit(rate.Inf)
	return "unlimited"
}

func (client *ExtentClientVer) Close() error {
	// release streamers
	var inodes []uint64
	client.streamerLock.Lock()
	inodes = make([]uint64, 0, len(client.streamers))
	for inode := range client.streamers {
		inodes = append(inodes, inode)
	}
	client.streamerLock.Unlock()
	for _, inode := range inodes {
		_ = client.EvictStream(inode)
	}
	client.dataWrapper.Stop()
	return nil
}

func (client *ExtentClientVer) AllocatePreLoadDataPartition(volName string, count int, capacity, ttl uint64, zones string) (err error) {
	return client.dataWrapper.AllocatePreLoadDataPartition(volName, count, capacity, ttl, zones)
}

func (client *ExtentClientVer) CheckDataPartitionExsit(partitionID uint64) error {
	_, err := client.dataWrapper.GetDataPartition(partitionID)
	return err
}

func (client *ExtentClientVer) GetDataPartitionForWrite() error {
	exclude := make(map[string]struct{})
	_, err := client.dataWrapper.GetDataPartitionForWrite(exclude)
	return err
}

func (client *ExtentClientVer) UpdateDataPartitionForColdVolume() error {
	return client.dataWrapper.UpdateDataPartition()
}

func (client *ExtentClientVer) IsPreloadMode() bool {
	return client.preload
}

func (client *extentClient) UploadFlowInfo(clientInfo wrapper.SimpleClientInfo) error {
	return client.dataWrapper.UploadFlowInfo(clientInfo, false)
}
