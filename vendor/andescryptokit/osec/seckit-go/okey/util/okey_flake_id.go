package util

import (
	"strconv"

	logs "andescryptokit/osec/seckit-go/log"

	"github.com/sony/sonyflake"
)

var flakeID *sonyflake.Sonyflake

// GenFlakeID generate a new snoy flake ID
func GenFlakeID() (int64, error) {
	id, err := flakeID.NextID()
	return int64(id), err
}

// InitSonyFlake ... initialize snoy flake id
func InitSonyFlake() {
	var st sonyflake.Settings

	flakeID = sonyflake.NewSonyflake(st)
	if flakeID == nil {
		logs.LogErrorfWithoutReqID("sonyflake not created")
	}
	logs.LogNoticefWithoutReqID("sonyflake created ok, sf:%v", flakeID)
}

// GenRandNumber ... length 18 or 19
func GenRandNumber() (string, error) {
	id, _ := flakeID.NextID()
	return strconv.FormatInt(int64(id), 10), nil
}
