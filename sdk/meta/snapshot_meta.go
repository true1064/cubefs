package meta

import (
	"github.com/cubefs/cubefs/proto"
	authSDK "github.com/cubefs/cubefs/sdk/auth"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/auth"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/time/rate"
	"sync"
	"time"
)

type MetaWrapper struct {
	*SnapShotMetaWrapper
}

type SnapShotMetaWrapper struct {
	*metaWrapper
	verInfo *proto.DelVer

	srcVer *proto.DelVer
	dstVer *proto.DelVer
}

func (mw *SnapShotMetaWrapper) CloneWrapper() *SnapShotMetaWrapper {
	return &SnapShotMetaWrapper{
		metaWrapper: mw.metaWrapper,
	}
}

type metaWrapper struct {
	sync.RWMutex
	cluster         string
	localIP         string
	volname         string
	ossSecure       *OSSSecure
	volCreateTime   int64
	owner           string
	ownerValidation bool
	mc              *masterSDK.MasterClient
	ac              *authSDK.AuthClient
	conns           *util.ConnectPool

	// Callback handler for handling asynchronous task errors.
	onAsyncTaskError AsyncTaskErrorFunc

	// Partitions and ranges should be modified together. So do not
	// use partitions and ranges directly. Use the helper functions instead.

	// Partition map indexed by ID
	partitions map[uint64]*MetaPartition

	// Partition tree indexed by Start, in order to find a partition in which
	// a specific inode locate.
	ranges *btree.BTree

	rwPartitions []*MetaPartition
	epoch        uint64

	totalSize  uint64
	usedSize   uint64
	inodeCount uint64

	authenticate bool
	Ticket       auth.Ticket
	accessToken  proto.APIAccessReq
	sessionKey   string
	ticketMess   auth.TicketMess

	closeCh   chan struct{}
	closeOnce sync.Once

	// Allocated to signal the go routines which are waiting for partition view update
	partMutex sync.Mutex
	partCond  *sync.Cond

	// Allocated to trigger and throttle instant partition updates
	forceUpdate         chan struct{}
	forceUpdateLimit    *rate.Limiter
	EnableSummary       bool
	metaSendTimeout     int64
	DirChildrenNumLimit uint32
	EnableTransaction   uint8
	TxTimeout           int64
	//EnableTransaction bool
	QuotaInfoMap map[uint32]*proto.QuotaInfo
	QuotaLock    sync.RWMutex
	Client       wrapper.SimpleClientInfo
	VerReadSeq   uint64
	LastVerSeq   uint64
}

func newMetaWrapper(config *MetaConfig) (*metaWrapper, error) {
	var err error
	mw := new(metaWrapper)
	mw.closeCh = make(chan struct{}, 1)

	if config.Authenticate {
		var ticketMess = config.TicketMess
		mw.ac = authSDK.NewAuthClient(ticketMess.TicketHosts, ticketMess.EnableHTTPS, ticketMess.CertFile)
		ticket, err := mw.ac.API().GetTicket(config.Owner, ticketMess.ClientKey, proto.MasterServiceID)
		if err != nil {
			return nil, errors.Trace(err, "Get ticket from authnode failed!")
		}
		mw.authenticate = config.Authenticate
		mw.accessToken.Ticket = ticket.Ticket
		mw.accessToken.ClientID = config.Owner
		mw.accessToken.ServiceID = proto.MasterServiceID
		mw.sessionKey = ticket.SessionKey
		mw.ticketMess = ticketMess
	}

	mw.volname = config.Volume
	mw.owner = config.Owner
	mw.ownerValidation = config.ValidateOwner
	mw.mc = masterSDK.NewMasterClient(config.Masters, false)
	mw.onAsyncTaskError = config.OnAsyncTaskError
	mw.metaSendTimeout = config.MetaSendTimeout
	mw.conns = util.NewConnectPool()
	mw.partitions = make(map[uint64]*MetaPartition)
	mw.ranges = btree.New(32)
	mw.rwPartitions = make([]*MetaPartition, 0)
	mw.partCond = sync.NewCond(&mw.partMutex)
	mw.forceUpdate = make(chan struct{}, 1)
	mw.forceUpdateLimit = rate.NewLimiter(1, MinForceUpdateMetaPartitionsInterval)
	mw.EnableSummary = config.EnableSummary
	mw.DirChildrenNumLimit = proto.DefaultDirChildrenNumLimit
	//mw.EnableTransaction = config.EnableTransaction
	mw.VerReadSeq = config.VerReadSeq

	limit := 0

	for limit < MaxMountRetryLimit {
		err = mw.initMetaWrapper()
		// When initializing the volume, if the master explicitly responds that the specified
		// volume does not exist, it will not retry.
		if err != nil {
			log.LogErrorf("NewMetaWrapper: init meta wrapper failed: volume(%v) err(%v)", mw.volname, err)
		}
		if err == proto.ErrVolNotExists {
			return nil, err
		}
		if err != nil {
			limit++
			time.Sleep(MountRetryInterval * time.Duration(limit))
			continue
		}
		break
	}

	if limit <= 0 && err != nil {
		return nil, err
	}
	go mw.updateQuotaInfoTick()
	go mw.refresh()
	return mw, nil
}

func NewSnapshotMetaWrapper(config *MetaConfig) (*SnapShotMetaWrapper, error) {
	mw, err := newMetaWrapper(config)
	return &SnapShotMetaWrapper{metaWrapper: mw}, err
}

func NewSnapshotMetaWrapperWith(mw *MetaWrapper) *SnapShotMetaWrapper {
	nmw := mw.SnapShotMetaWrapper.metaWrapper
	return &SnapShotMetaWrapper{metaWrapper: nmw}
}

func (mw *SnapShotMetaWrapper) SetVerInfo(info *proto.DelVer) {
	mw.verInfo = info
}

func (mw *SnapShotMetaWrapper) GetVerInfo() *proto.DelVer {
	return mw.verInfo
}

func (mw *SnapShotMetaWrapper) SetRenameVerInfo(src, dst *proto.DelVer) {
	mw.srcVer = src
	mw.dstVer = dst
}
