package util

import (
	"crypto/rand"

	errors "github.com/pkg/errors"
)

// GenerateRand generates random number
func GenerateRand(reqID string, bitLength int) (rn []byte, err error) {
	if bitLength%8 != 0 {
		err = errors.Errorf("GenerateRand: bitLength should multiple of 8 bits, reqid(%v)", reqID)
		return nil, err
	}

	rn = make([]byte, bitLength/8)
	_, err = rand.Read(rn)
	if nil != err {
		err = errors.WithMessage(err, "GenerateRand")
		return nil, err
	}

	return rn, err
}
