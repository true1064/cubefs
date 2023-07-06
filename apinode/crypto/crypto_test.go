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
		"CtgCSkRmNXlVRDUzYTkvUy9GRDJhVTBSdGNFU0dOeUNHTEhqWXhUMlN2Ym5CVUp5YXQ4eVdHVDArNVRh" +
		"MlF4M00rZTdoazQ3dFhEYThQK0xSV2NlWC9IbzdML3pzL1dBdSs1Y0dUUUp3c1FhRE5rNGVab25xRXpt" +
		"U2c0SHJ0UkEwWklrVDcrL0ZsOGowOWlNSjkwc2QrY3Rpc3djcEFhcTZORzRFTEorc0RxREVBYjhGTkFi" +
		"TTNUQXMvN2dXelpSM0tIQjB6aG1Wa3pNL1J5anQxSkVQUFpYR0FaVmRTdExFNzRXN21ZVGhySzBXeERG" +
		"ZzhqQ25ZUlRQQXJLMnl0cHA0c01ER1V0MEp1Y2doUlA4UHBuVDlvT2JGY2NsNHAyenFhZjh3aGdoWkFD" +
		"c2JDUjRkZFFCdnNzenZIazc0dnhac0V0OG5CNDlNK0U3WjBmdVRpR2x5K2tBPT0SGGlFWEJZMjBTbnhW" +
		"SStva1RaMG9FSlE9PRgC"
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
				c := NewCryptor()
				b.ResetTimer()
				for ii := 0; ii <= b.N; ii++ {
					r := bytes.NewBuffer(buffer)
					cr, err := c.TransEncryptor(material, r)
					require.NoError(b, err)
					pr, err := c.TransDecryptor(material, cr)
					require.NoError(b, err)
					io.Copy(io.Discard, pr)
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
