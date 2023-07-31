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
	kit "andescryptokit"
	"andescryptokit/engine"
	"andescryptokit/errno"
	"andescryptokit/types"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"sync"

	"github.com/cubefs/cubefs/apinode/sdk"
)

const (
	// EncryptMode alias of engine mode.
	EncryptMode = types.ENCRYPT_MODE
	// DecryptMode alias of engine mode.
	DecryptMode = types.DECRYPT_MODE
)

var (
	once      sync.Once
	cryptoKit kit.AndesCryptoKit

	// BlockSize independent block with crypto.
	BlockSize uint64 = 4096
	pool             = sync.Pool{
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
		Status:  sdk.ErrServerCipher.Status,
		Code:    sdk.ErrServerCipher.Code,
		Message: fmt.Sprintf("%d: %s", en.Code(), en.Error()),
	}
}

func initOnce() error {
	var err *errno.Errno
	once.Do(func() {
		configure := types.Configure{
			Environment: types.EnvironmentTest,
			AuthParam: types.AuthParam{
				AppName: "cryptokit",
				AK:      "AK0642C908CE001356",
				SK:      "cded5f1a55747351dbf4435a4f8fab02f803ee74fd4518fcd1bec1311bdadf79",
			},
			CustomMasterKey: types.CustomMasterKey{
				FileKeyId:  "a08be12a-08df-4754-a7b4-ff69c0fac73e",
				TransKeyId: "a67d2abb-c7f6-408d-93d7-450c8394e73e",
			},
		}
		if cryptoKit, err = kit.New(types.CipherScheme_ServiceBasedKMS, configure); err != errno.OK {
			return
		}
	})
	return fileError(err)
}

// Init init server client of crypto kit.
func Init() error {
	return initOnce()
}

// Cryptor for transmitting and file content.
type Cryptor interface {
	Transmitter(material string) (trans Transmitter, err error)
	// Trans encrypt and decrypt every byte.
	TransEncryptor(material string, plaintexter io.Reader) (ciphertexter io.Reader, newMaterial string, err error)
	TransDecryptor(material string, ciphertexter io.Reader) (plaintexter io.Reader, err error)

	// File encrypt and decrypt every BlockSize bytes.
	GenKey() (key []byte, err error)
	FileEncryptor(key []byte, plaintexter io.Reader) (ciphertexter io.Reader, err error)
	FileDecryptor(key []byte, ciphertexter io.Reader) (plaintexter io.Reader, err error)
}

// Transmitter for block bytes and reader transmitting.
type Transmitter interface {
	// Encrypt and Decrypt is thread-safe.
	Encrypt(plaintext string, encode bool) (ciphertext string, err error)
	Decrypt(ciphertext string, decode bool) (plaintext string, err error)
}

type transmitter struct {
	material string
	engine   *engine.EngineAesGCMCipher
}

var _ Transmitter = (*transmitter)(nil)

func (t *transmitter) Encrypt(plaintext string, encode bool) (string, error) {
	if t.material == "" {
		if encode {
			return hex.EncodeToString([]byte(plaintext)), nil
		}
		return plaintext, nil
	}

	data, en := t.engine.Encrypt([]byte(plaintext))
	if en != errno.OK {
		return "", transError(en)
	}
	if encode {
		return hex.EncodeToString(data), nil
	}
	return string(data), nil
}

func (t *transmitter) Decrypt(ciphertext string, decode bool) (string, error) {
	var (
		buff []byte
		err  error
	)
	if decode {
		buff, err = hex.DecodeString(ciphertext)
		if err != nil {
			return "", sdk.ErrBadRequest.Extend("hex:", ciphertext)
		}
	} else {
		buff = []byte(ciphertext)
	}

	if t.material == "" {
		return string(buff), nil
	}

	data, en := t.engine.Decrypt(buff)
	if en != errno.OK {
		return "", transError(en)
	}
	return string(data), nil
}

// NoneCryptor new Cryptor without init.
func NoneCryptor() Cryptor {
	return cryptor{}
}

// NewCryptor returns the encryption and decryption object.
func NewCryptor() Cryptor {
	if err := initOnce(); err != nil {
		panic(err)
	}
	return cryptor{}
}

func newTransReader(mode types.CipherMode, material string, r io.Reader) (*engine.EngineTransCipher, *errno.Errno) {
	key, derr := base64.StdEncoding.DecodeString(material)
	if derr != nil {
		return nil, errno.TransCipherIVBase64DecodeError.Append(derr.Error())
	}
	return cryptoKit.NewEngineTransCipher(mode, key, r)
}

type cryptor struct{}

func (cryptor) TransEncryptor(material string, plaintexter io.Reader) (io.Reader, string, error) {
	if len(material) == 0 {
		return plaintexter, "", nil
	}
	r, err := newTransReader(EncryptMode, material, plaintexter)
	if errx := transError(err); errx != nil {
		return nil, "", nil
	}
	return r, base64.StdEncoding.EncodeToString(r.GetCipherMaterial()), nil
}

func (cryptor) TransDecryptor(material string, ciphertexter io.Reader) (io.Reader, error) {
	if len(material) == 0 {
		return ciphertexter, nil
	}
	r, err := newTransReader(DecryptMode, material, ciphertexter)
	return r, transError(err)
}

func (cryptor) Transmitter(material string) (Transmitter, error) {
	trans := &transmitter{material: material}
	if material != "" {
		key, derr := base64.StdEncoding.DecodeString(material)
		if derr != nil {
			return nil, transError(errno.TransCipherIVBase64DecodeError.Append(derr.Error()))
		}
		eng, err := cryptoKit.NewEngineAesGCMCipher(key)
		if err != errno.OK {
			return trans, transError(err)
		}
		trans.engine = eng
	}
	return trans, nil
}

func (cryptor) GenKey() ([]byte, error) {
	cipher, err := cryptoKit.NewEngineFileCipher(EncryptMode, nil, io.MultiReader(), uint64(BlockSize))
	if err != errno.OK {
		return nil, fileError(err)
	}
	return cipher.GetCipherMaterial(), nil
}

func (cryptor) FileEncryptor(key []byte, plaintexter io.Reader) (io.Reader, error) {
	if key == nil {
		return plaintexter, nil
	}
	cipher, err := cryptoKit.NewEngineFileCipher(EncryptMode, key, plaintexter, uint64(BlockSize))
	if err != errno.OK {
		return nil, fileError(err)
	}
	return &fileCryptor{
		offset:  -1,
		block:   pool.Get().([]byte)[:BlockSize],
		reader:  plaintexter,
		cryptor: cipher.EncryptBlock,
	}, nil
}

func (cryptor) FileDecryptor(key []byte, ciphertexter io.Reader) (io.Reader, error) {
	if key == nil {
		return ciphertexter, nil
	}
	cipher, err := cryptoKit.NewEngineFileCipher(DecryptMode, key, ciphertexter, uint64(BlockSize))
	if err != errno.OK {
		return nil, fileError(err)
	}
	return &fileCryptor{
		offset:  -1,
		block:   pool.Get().([]byte)[:BlockSize],
		reader:  ciphertexter,
		cryptor: cipher.DecryptBlock,
	}, nil
}

type fileCryptor struct {
	once    sync.Once
	offset  int
	block   []byte
	reader  io.Reader
	err     error
	cryptor func([]byte, []byte, uint64) *errno.Errno
}

func (r *fileCryptor) free() {
	r.once.Do(func() {
		if r.block != nil {
			block := r.block
			r.block = nil
			pool.Put(block) // nolint: staticcheck
		}
	})
}

func (r *fileCryptor) Read(p []byte) (n int, err error) {
	if r.err != nil {
		r.free()
		return 0, r.err
	}

	for len(p) > 0 {
		if r.offset < 0 || r.offset == len(r.block) {
			if r.err = r.nextBlock(); r.err != nil {
				if n > 0 {
					return n, nil
				}
				r.free()
				return n, r.err
			}
		}

		read := copy(p, r.block[r.offset:])
		r.offset += read

		p = p[read:]
		n += read
	}
	return n, nil
}

func (r *fileCryptor) nextBlock() error {
	n, err := readFullOrToEnd(r.reader, r.block)
	r.offset = 0
	r.block = r.block[:n]
	if n > 0 {
		if eno := r.cryptor(r.block, r.block, 0); eno != errno.OK {
			return fileError(eno)
		}
	}
	return err
}

func readFullOrToEnd(r io.Reader, buffer []byte) (n int, err error) {
	nn, size := 0, len(buffer)

	for n < size && err == nil {
		nn, err = r.Read(buffer[n:])
		n += nn
		if n != 0 && err == io.EOF {
			return n, nil
		}
	}

	return n, err
}
