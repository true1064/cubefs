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
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

const (
	kb = 1 << 10
	mb = 1 << 20

	material string = "" +
		"CoACxgB8YF+43uKf1J7yuVtL1vfYHUYS3/s1ek5LOchxCen8YIRvumRPTieThba/iiO7dlnwBlFsey4U" +
		"IO4AByGMx3+xhru04q3YCfN/YCf9H/8mMkwldsOafMHgs9J9PhGA6B0w84EIQJk72jAelj+qPJ/eEm1M" +
		"m1uSt0m/xf+whKmQgkma3Hp0C93nhc8yKc6PczD5onQqrtqJIgf5xTcGC7ebEd3Qge0lwwxIcB0Y1cD/" +
		"rxHtgRXhk+kzZSifVYYDp8WcZ0itV2X3QFHNvadDSnoGzyWZq/er7KLHP4xJvXq3dlZ3xODON4N7W6IO" +
		"pE0DPuput+K2RackS9wrgXWc3xIQqtBvoklkzifqz46BN5djsxo9AAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
		"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADgB"
)

var (
	blocks = []uint64{kb, kb * 4, kb * 16, kb * 64, kb * 512, mb}
	sizes  = []uint64{128, kb * 4, kb * 128, mb, mb * 4, mb * 16}
)

func name(block, size uint64) string {
	return humanize.IBytes(block) + "-" + humanize.IBytes(size)
}

func timed(t time.Time, n int) string {
	return duration(time.Since(t), n)
}

func duration(d time.Duration, n int) string {
	var dur int64
	var idx int
	var unit string
	e := [3]int64{1e6, 1e3, 1}
	for idx, unit = range [3]string{"ms", "us", "ns"} {
		if dur = int64(d) / e[idx]; dur > 0 {
			break
		}
	}
	return fmt.Sprintf("%d%s(%.2f%s)", dur, unit, float64(dur)/float64(n), unit)
}

func setBlock(block uint64) {
	BlockSize = block
	cacheSize = 32 * int(BlockSize)
	pool = sync.Pool{
		New: func() interface{} {
			return make([]byte, cacheSize)
		},
	}
}

func TestCryptoGCM(t *testing.T) {
	require.NoError(t, Init(Configure{}))

	c := NewCryptor()
	st := time.Now()
	const n = 200
	for range [n]struct{}{} {
		_, err := c.Transmitter(material)
		require.NoError(t, err)
	}
	t.Logf("CreateGCM: n=%d, duration=%s", n, timed(st, n))

	trans, err := c.Transmitter(material)
	require.NoError(t, err)
	for _, size := range []uint64{1 << 9, 1 << 10, 1 << 20} {
		buff := make([]byte, size)
		rand.Read(buff)
		ciphertext, err := trans.Encrypt(string(buff), true)
		require.NoError(t, err)

		st = time.Now()
		for range [n]struct{}{} {
			_, err := trans.Decrypt(ciphertext, true)
			require.NoError(t, err)
		}
		t.Logf("CipherGCM-%s: n=%d, duration=%s", humanize.IBytes(size), n, timed(st, n))
	}
}

func TestCryptoStream(t *testing.T) {
	require.NoError(t, Init(Configure{}))

	c := NewCryptor()
	_, err := c.GenKey()
	require.NoError(t, err)

	st := time.Now()
	const n = 200
	for range [n]struct{}{} {
		_, err := c.TransDecryptor(material, io.MultiReader())
		require.NoError(t, err)
	}
	t.Logf("CreateStream: n=%d, duration=%s", n, timed(st, n))

	for _, size := range []uint64{1 << 9, 1 << 10, 512 << 10, 1 << 20} {
		buff := make([]byte, size)
		buffr := make([]byte, size)
		rand.Read(buff)
		var d time.Duration
		for range [n]struct{}{} {
			pr, err := c.TransDecryptor(material, bytes.NewBuffer(buff))
			require.NoError(t, err)
			st = time.Now()
			_, err = io.ReadFull(pr, buffr)
			d += time.Since(st)
			require.NoError(t, err)
		}
		t.Logf("CipherStream-%s: n=%d, duration=%s", humanize.IBytes(size), n, duration(d, n))
	}
}

func BenchmarkTransmit(b *testing.B) {
	require.NoError(b, Init(Configure{}))
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
	require.NoError(b, Init(Configure{}))
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
