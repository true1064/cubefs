// Copyright 2023 The CubeFS Authors.
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

package crypto

import (
	"bytes"
	"io"
	"sync"
	"testing"

	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

const (
	kb = 1 << 10
	mb = 1 << 20

	material string = "" +
		"CoACa0/B6pPpInXLffXsD4RWzAe7JMK94E9pzmMbT8Uq1E0GyiDJ1ssku+U6U3nPHqeMON83vIoeMWey" +
		"v3jzjEuOgV/cm3IDZe8d6O3mWBZ6F45+LjR/Sc0Gw8Hax3zcsonvTIljWzbiwhEQSw+Wlo5vbkU6IYfS" +
		"7XHtffyNv1znf9FCY6n9da/MbbNDH0z3jbiy8PZgIucFitna8U2UU6l/U4Az3TRhhm/sEwRuONPgQV4i" +
		"+rbCbR0cVJsqaLaRfPCPwO1TlMfEIlpejucZyrHdF2HUanuwjIG6EqAlfLaVY6/Occ+XQlE31GExCMFx" +
		"wk3FYKKkWIg4aKijdOOAsOUfoBIQZjxuARdr1C24eHatCjp0vRo8TAKhluGM6jPoTI/sWPe7rJRqXbVP" +
		"dSmmzWgPKmxiJ3Pz0Fv1t/UamhB0HzsvpJHg4Bk4H1Vlyc0vQJH4OAE="
)

var (
	blocks = []uint64{kb, kb * 4, kb * 16, kb * 64, kb * 512, mb}
	sizes  = []uint64{128, kb * 4, kb * 128, mb, mb * 4, mb * 16}
)

func name(block, size uint64) string {
	return humanize.IBytes(block) + "-" + humanize.IBytes(size)
}

func setBlock(block uint64) {
	BlockSize = block
	pool = sync.Pool{
		New: func() interface{} {
			return make([]byte, BlockSize)
		},
	}
}

func BenchmarkTransmit(b *testing.B) {
	for _, block := range blocks {
		setBlock(block)
		for _, size := range sizes {
			b.Run(name(block, size), func(b *testing.B) {
				buffer := make([]byte, size)
				newBuff := make([]byte, size)
				c := NewCryptor()
				b.ResetTimer()
				for ii := 0; ii <= b.N; ii++ {
					r := bytes.NewBuffer(buffer)
					cr, newMaterial, err := c.TransEncryptor(material, r)
					require.NoError(b, err)
					pr, err := c.TransDecryptor(newMaterial, cr)
					require.NoError(b, err)
					_, err = io.ReadFull(pr, newBuff)
					require.NoError(b, err)
					require.Equal(b, buffer, newBuff)
				}
			})
		}
	}
}

func BenchmarkCrypto(b *testing.B) {
	for _, block := range blocks {
		setBlock(block)
		for _, size := range sizes {
			b.Run(name(block, size), func(b *testing.B) {
				buffer := make([]byte, size)
				c := NewCryptor()
				key, err := c.GenKey()
				require.NoError(b, err)
				b.ResetTimer()
				for ii := 0; ii <= b.N; ii++ {
					r := bytes.NewBuffer(buffer)
					cr, err := c.FileEncryptor(key, r)
					require.NoError(b, err)
					pr, err := c.FileDecryptor(key, cr)
					require.NoError(b, err)
					io.Copy(io.Discard, pr)
				}
			})
		}
	}
}
