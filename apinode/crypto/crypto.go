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
	"encoding/base64"
	"fmt"
	"io"
	"sync"

	"github.com/cubefs/cubefs/apinode/sdk"
	kit "oppo.com/andes-crypto/kit/server"
	"oppo.com/andes-crypto/kit/server/engine"
	"oppo.com/andes-crypto/kit/server/errno"
)

const (
	// BlockSize independent block with crypto.
	BlockSize = 4096
)

var (
	once      sync.Once
	cryptoKit kit.AndesCryptoKit

	pool = sync.Pool{
		New: func() interface{} {
			return make([]byte, BlockSize)
		},
	}
)

func transError(en *errno.Errno) error {
	if en == nil || en == errno.OK {
		return nil
	}
	return &sdk.Error{
		Status:  sdk.ErrTransCipher.Status,
		Code:    sdk.ErrTransCipher.Code,
		Message: fmt.Sprintf("%d: %s", en.Code(), en.Error()),
	}
}

func fileError(en *errno.Errno) error {
	if en == nil || en == errno.OK {
		return nil
	}
	return &sdk.Error{
		Status:  500,
		Code:    "CryptoError",
		Message: fmt.Sprintf("%d: %s", en.Code(), en.Error()),
	}
}

func initOnce() error {
	var err *errno.Errno
	once.Do(func() {
		if cryptoKit, err = kit.New(kit.CipherScheme_Service); err != errno.OK {
			return
		}

		kmsParam := kit.KmsParam{
			AppName: "cryptokit",
			AK:      "AK0642C908CE001356",
			SK:      "cded5f1a55747351dbf4435a4f8fab02f803ee74fd4518fcd1bec1311bdadf79",
			KeyId:   "a08be12a-08df-4754-a7b4-ff69c0fac73e",
		}
		err = cryptoKit.Init(kmsParam, kit.EnvironmentTest)
	})
	return fileError(err)
}

// Init init server client of crypto kit.
func Init() error {
	return initOnce()
}

// TransCipher cipher for transmitting.
type TransCipher interface {
	Encryptor(material string, plaintexter io.Reader) (ciphertexter io.Reader, err error)
	Decryptor(material string, ciphertexter io.Reader) (plaintexter io.Reader, err error)
}

// FileCipher cipher for file content.
type FileCipher interface {
	Encryptor(key []byte, plaintexter io.Reader) (ciphertexter io.Reader)
	Decryptor(key []byte, ciphertexter io.Reader) (plaintexter io.Reader)
}

// NewTransCipher returns the transfer encryption and decryption object.
func NewTransCipher() TransCipher {
	if err := initOnce(); err != nil {
		panic(err)
	}
	return transCipher{}
}

// MockTransCipher for testing.
func MockTransCipher() TransCipher {
	return transCipher{}
}

func newTransReader(mode engine.CipherMode, material string, r io.Reader) (io.Reader, *errno.Errno) {
	if len(material) == 0 {
		return r, nil
	}

	key, derr := base64.StdEncoding.DecodeString(material)
	if derr != nil {
		return nil, errno.TransCipherIVBase64DecodeError.Append(derr.Error())
	}
	if len(key) != 256 {
		return nil, errno.TransCipherIVBase64DecodeError
	}

	cipher, err := cryptoKit.NewEngineTransCipherStream(mode, io.MultiReader(bytes.NewReader(key), r))
	if err != nil {
		return nil, err
	}
	return cipher, nil
}

type transCipher struct{}

func (transCipher) Encryptor(material string, plaintexter io.Reader) (io.Reader, error) {
	r, err := newTransReader(engine.ENCRYPT_MODE, material, plaintexter)
	return r, transError(err)
}

func (transCipher) Decryptor(material string, ciphertexter io.Reader) (io.Reader, error) {
	r, err := newTransReader(engine.DECRYPT_MODE, material, ciphertexter)
	return r, transError(err)
}

// NewFileCipher returns the file encryption and decryption object.
func NewFileCipher() (FileCipher, error) {
	if err := initOnce(); err != nil {
		return nil, err
	}
	return fileCipher{}, nil
}

type fileCipher struct{}

type blockCrypt func([]byte, uint64) ([]byte, *errno.Errno)

type cryptor struct {
	err    error
	reader io.Reader
	fn     blockCrypt

	once   sync.Once
	remain []byte
}

func (e *cryptor) free() {
	e.once.Do(func() {
		pool.Put(e.remain) // nolint: staticcheck
	})
}

func (e *cryptor) Read(p []byte) (n int, err error) {
	if e.err != nil {
		e.free()
		err = e.err
		return
	}

	for len(p) > 0 {
		// read remain firstly, return if it still have remaining.
		if l := len(e.remain); l > 0 {
			cn := copy(p, e.remain)
			e.remain = e.remain[cn:]
			p = p[cn:]
			if cn < l {
				return cn, nil
			}
		}

		e.nextBlock()
	}

	buff := e.remain[0:BlockSize]
	for len(p) > 0 {
		n, err = e.reader.Read(buff)
		if err != nil {
			return
		}
	}
	return
}

func (e *cryptor) nextBlock() {
	if len(e.remain) > 0 {
		return
	}
}

func (fileCipher) Encryptor(key []byte, plaintexter io.Reader) (ciphertexter io.Reader) {
	cipher, _, err := cryptoKit.NewEngineFileCipher(key, uint64(BlockSize))

	var fn blockCrypt
	if cipher != nil {
		fn = cipher.EncryptBlock
	}

	buff := pool.Get().([]byte)
	buff = buff[:0]
	return &cryptor{
		err:    fileError(err),
		reader: plaintexter,
		fn:     fn,
		remain: buff,
	}
}

func (fileCipher) Decryptor(key []byte, ciphertexter io.Reader) (plaintexter io.Reader) {
	return nil
}
