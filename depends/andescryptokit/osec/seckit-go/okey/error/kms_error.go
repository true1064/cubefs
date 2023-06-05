package kmserror

import (
	"errors"
)

var (
	// ErrParasInvalid
	ErrRepeatInit      = errors.New("repeat init")
	ErrParasInvalid    = errors.New("invalid Parameter")
	ErrAuthInit        = errors.New("auth init failed")
	ErrJsonFormat      = errors.New("json format invalid")
	ErrHexString       = errors.New("hex string invalid")
	ErrMaxApps         = errors.New("max apps")
	ErrClientPost      = errors.New("client post failed")
	ErrPackDataInvalid = errors.New("pack data invalid")
	ErrInternal        = errors.New("internal error")
)
