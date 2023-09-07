package stream

import (
	"container/list"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/manager"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/time/rate"
	"strings"
	"sync"
	"time"
)

type extentClient struct {
	streamers          map[uint64]*Streamer
	streamerList       *list.List
	streamerLock       sync.Mutex
	maxStreamerLimit   int
	readLimiter        *rate.Limiter
	writeLimiter       *rate.Limiter
	disableMetaCache   bool
	volumeType         int
	volumeName         string
	bcacheEnable       bool
	bcacheDir          string
	BcacheHealth       bool
	preload            bool
	LimitManager       *manager.LimitManager
	dataWrapper        *wrapper.Wrapper
	inflightL1cache    sync.Map
	inflightL1BigBlock int32
	multiVerMgr        *MultiVerMgr

	evictBcache EvictBacheFunc
	loadBcache  LoadBcacheFunc
	cacheBcache CacheBcacheFunc
	evictIcache EvictIcacheFunc //May be null, must check before using
}

type ExtentClientVer struct {
	*extentClient
	appendExtentKey AppendExtentKeyFunc
	splitExtentKey  SplitExtentKeyFunc
	getExtents      GetExtentsFunc
	truncate        TruncateFunc
}

func newExtentClient(config *ExtentConfig) (client *extentClient, err error) {
	client = new(extentClient)
	client.LimitManager = manager.NewLimitManager(client)
	client.LimitManager.WrapperUpdate = client.UploadFlowInfo
	limit := 0
retry:
	client.dataWrapper, err = wrapper.NewDataPartitionWrapper(client, config.Volume, config.Masters, config.Preload, config.VerReadSeq)
	if err != nil {
		log.LogErrorf("NewExtentClient: new data partition wrapper failed: volume(%v) mayRetry(%v) err(%v)",
			config.Volume, limit, err)
		if strings.Contains(err.Error(), proto.ErrVolNotExists.Error()) {
			return nil, proto.ErrVolNotExists
		}
		if limit >= MaxMountRetryLimit {
			return nil, errors.Trace(err, "Init data wrapper failed!")
		} else {
			limit++
			time.Sleep(MountRetryInterval * time.Duration(limit))
			goto retry
		}
	}

	client.streamers = make(map[uint64]*Streamer)
	client.multiVerMgr = &MultiVerMgr{}
	client.dataWrapper.InitFollowerRead(config.FollowerRead)
	client.dataWrapper.SetNearRead(config.NearRead)
	client.volumeType = config.VolumeType
	client.volumeName = config.Volume
	client.bcacheEnable = config.BcacheEnable
	client.bcacheDir = config.BcacheDir
	client.multiVerMgr.verReadSeq = config.VerReadSeq
	client.BcacheHealth = true
	client.preload = config.Preload
	client.disableMetaCache = config.DisableMetaCache

	var readLimit, writeLimit rate.Limit
	if config.ReadRate <= 0 {
		readLimit = defaultReadLimitRate
	} else {
		readLimit = rate.Limit(config.ReadRate)
	}
	if config.WriteRate <= 0 {
		writeLimit = defaultWriteLimitRate
	} else {
		writeLimit = rate.Limit(config.WriteRate)
	}
	client.readLimiter = rate.NewLimiter(readLimit, defaultReadLimitBurst)
	client.writeLimiter = rate.NewLimiter(writeLimit, defaultWriteLimitBurst)

	client.evictBcache = config.OnEvictBcache
	client.evictIcache = config.OnEvictIcache
	client.loadBcache = config.OnLoadBcache
	client.cacheBcache = config.OnCacheBcache

	if config.MaxStreamerLimit <= 0 {
		client.disableMetaCache = true
		return
	}

	if config.MaxStreamerLimit <= defaultStreamerLimit {
		client.maxStreamerLimit = defaultStreamerLimit
	} else if config.MaxStreamerLimit > defMaxStreamerLimit {
		client.maxStreamerLimit = defMaxStreamerLimit
	} else {
		client.maxStreamerLimit = int(config.MaxStreamerLimit)
	}

	client.maxStreamerLimit += fastStreamerEvictNum

	log.LogInfof("max streamer limit %d", client.maxStreamerLimit)
	client.streamerList = list.New()
	go client.backgroundEvictStream()

	return client, nil
}

func NewExtentClientVer(config *ExtentConfig) (client *ExtentClientVer, err error) {
	cli, err := newExtentClient(config)
	if err != nil {
		return nil, err
	}

	client = &ExtentClientVer{
		extentClient: cli,
	}

	client.appendExtentKey = config.OnAppendExtentKey
	client.splitExtentKey = config.OnSplitExtentKey
	client.getExtents = config.OnGetExtents
	client.truncate = config.OnTruncate

	return client, nil
}

func (client *ExtentClientVer) Clone() *ExtentClientVer {
	return &ExtentClientVer{extentClient: client.extentClient}
}

func (client *ExtentClientVer) UpdateFunc(mw *meta.SnapShotMetaWrapper) {
	client.appendExtentKey = mw.AppendExtentKey
	client.splitExtentKey = mw.SplitExtentKey
	client.getExtents = mw.GetExtents
	client.truncate = mw.Truncate
}
