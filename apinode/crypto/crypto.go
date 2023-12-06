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
	BlockSize uint64 = 4096 // 4 KB

	cacheSize = 32 * int(BlockSize) // 128 KB
	pool      = sync.Pool{
		New: func() interface{} {
			return make([]byte, cacheSize)
		},
	}
)

// Configure crypto configures.
type Configure struct {
	Environment uint16 `json:"environment"`
	AppName     string `json:"appName"`
	AK          string `json:"ak"`
	SK          string `json:"sk"`
	FileKeyID   string `json:"fileKeyId"`
	TransKeyID  string `json:"transKeyId"`

	CipherPrivateKey  string `json:"cipherPrivateKey"`
	CipherPrivatePath string `json:"cipherPrivatePath"`
}

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

func initOnce(conf Configure) error {
	empty := Configure{}
	if conf == empty {
		conf.Environment = uint16(types.EnvironmentTest)
		conf.AppName = "cfa-client"
		conf.AK = "AK06A9C42F20001356"
		conf.SK = "51c03043bf791f2b122a32ac9dfef335ec06aa583509a2dbf895153c65c1f2fb"
		conf.FileKeyID = "24b7a5ff-5a82-4606-b394-94755798deab"
		conf.TransKeyID = "dc907f16-8cec-4873-bc78-bb227868a0ef"
		conf.CipherPrivateKey = "" +
			"eyJ2ZXJzaW9uIjowLCJhbGciOiJBRVMtMjU2LUdDTSIsInBhZGRpbmciOiJOb1BhZGRpbmciLCJpdiI6" +
			"IndwNUt2dmdLT3ZtWm1vNXgiLCJhYWQiOiIiLCJ0YWciOiIxelJwMEZCdjlTQzlTSldvYkdkU29BPT0i" +
			"LCJlbmNyeXB0ZWRfZGF0YSI6InByR0kvdzByMjRkbEkyREZkRlFwSEYxYjRQd1ZMcGRPVGJCdzZIdHJ5" +
			"NWNHL0dMamtkSVRhNVBSZURaZ1A1UmlsSjluemh0eWdVZFdpKzdyVXZaRTJqd1ZBb092cW1uRFVNN2RM" +
			"MytjbURFMEY5cW0vVmEvaHQrSVlaR2hCS0ZQNTBubGRINmNQWW1Pdm13aCtHTXVnekYzNXRER1RNeW1t" +
			"dWRvUisvdEd6OWpYNW82NWRhVkFtcG9lWEs1UnllN2dQQ29jQ05NcmF2NWRWSzZlejQzdW8rVUNtRlZw" +
			"Y0JiQlpDTHA3d08vSWRoUTRlRDdLSkh1QWEzRzMrNHhXOHBUemVBeE1SVjF6aTRKVHFvSjJ0dmsvV240" +
			"L0Iwc2JKZFo2SGttMUM4aCtZcHpMczFaVitNMUNuWi9QbXNZTng4bXk3cHA5VEUyaWtYVGJXdUhIQmo5" +
			"ZTlJLzF3U0UxckRmRHpwazZSZ0Y4cmdJREdvcFk5bUpnWS9QQnd5aGdUU1ptam5Sa2p3dmtMZXQrU05r" +
			"RTdHRytacytmUmNNdU0xUUpidzZ0WHF6ZCs3bGNpSEUxWGErUG91UlEwUjhZeG5IdTFqVjZ5RWhoTW12" +
			"dEs1N3drTXJONE4wQlE3K0NJcGdleWxldmZSVC9tUWJNTXgzV0JhL3M5Ti95UlpsUGJhNzRiS1k0Yi9n" +
			"QjBraFlGVnI3cUhKbVV4UVRKNjVRcVJuWWJaZUFYNFQyYzdQY3VFYld4aXkrQndDMEw5bG9OK21ERy91" +
			"UnhPUllkVE95V0RERzVaRVBCMXAvVnp2VFQvSElHYWJhYld2S2ZKaHFVOGpHcnExNWVwdjFQbEVrdnB5" +
			"M1JGOVNGZDhobzdWdkNuV2hzSjBzY0dQMFArUnB3cUtuMkFjWGd3R0tGNEVhVGpDS1A5NFlGdUx1MmJX" +
			"WlZxZm9vei8xV2kycDFweVY1Q2ZtejdHbDJ2aHJMdExVNThLU3o5NjZ2RG9TcGhKb3Q2djhSYjhuNXRC" +
			"YlEvUnRPeG9kOVV6bEpGY0tqZS9rNFM1WElsZWJUME9lQXBtbThzRG9XRG9KZzFLbStRV2U0d0pSdGZV" +
			"S2VKV0c0b0k4N29KRm5LU1dvczZ4clFvRUZ6aTR4WGNzVWNIaGtEMmVPSkRSdTNkOEhDK1RQWksyK2M3" +
			"T0R3L3NjZmxEb1F5bDBvMTMvczlxM3VRblF3MTZ4VHRnRUZ5OWhSQTdGZERxR2VzeURVWUc3MldoWVh1" +
			"M2pDd2dua09ENkFjV1dDOVROei9kTHNQU0FUTEMveFBlQzdHS2NOeC9lODlldm04M1F2YXhEY2dIYnRs" +
			"Z2NrNkc5MmQ0dml5Uzloc2JXbHU2ZU1oY1QvQXJDK2NZZnJCWDhtek90R2Rqc2JrRHhWa0FNVEowT2Zx" +
			"bzloK3A3SzNXRmNLeU45YkpNUWQ1aUtpSndRUXRsaTB1bGxHZU5TU2VldVZURWJiR0xGNUVDL010QnlO" +
			"UHhCWkl3RDZaaWp3aWRHemNQZHhnTzBZbXVlTEZ6L0U2MEhlWWhrZ1hQZGE4MGVubnZUcitiOXQ0bEk1" +
			"THdnYVZyK2x5cldzVHVWR0tETnB5MUpXcFVRaTFvaUtwL29tVHJJQXZTcjN6dkhXOE5UcGpXNmFiLzd1" +
			"NWJVa21uTFlOTUhzUjIwdjBsREJndzhCV3BPTi9hSUU4bmJZNEFjWmczcFNBcm5KMEtMVm5hczJETlhB" +
			"Z2pNdTgwNHFwSEdiSnczR2IrbjJLejc4WWMxSy8vMWU4cERpaDlva1h4WWViVFMwcGZyUVJKN0lSUHpG" +
			"UGpaNzB2UVBhOTZoSVhOTG9lQUltdGdIZjhXVlZua2xWTFBocEttQWtKazNCZGlBZG5oNkNiQ0lmR0Zm" +
			"RXdqSFRVSWJoVCswOFJSaExVclg4VnNOYnM1OTkzRkF0WTkrWnBOREJ0SERtNWlYMU1XWkNXdlQ1OVh1" +
			"UmxEa2tUbXRVZzNMUUpPRXBwbWdEc2R3dG8vODlCcWxJalFvcGFmSjhtRmdQTE5iZTVGZDJNcEYzRmFp" +
			"RkVRWUQ2TUN2WWp6SzNyNlRYUFU0VjlxcVZkRUx4cDZoZDBFblZMTnpCYVQ3dXlYNzA5bEJkckQyamlk" +
			"S3dGM20wZmhNVmpnVmFGd0lOaDlwaz0ifQ=="
	}

	var err *errno.Errno
	once.Do(func() {
		configure := types.Configure{
			Environment: types.EnvironmentType(conf.Environment),
			AuthParam: types.AuthParam{
				AppName: conf.AppName,
				AK:      conf.AK,
				SK:      conf.SK,
			},
			CustomMasterKey: types.CustomMasterKey{
				FileKeyId:        conf.FileKeyID,
				TransKeyId:       conf.TransKeyID,
				CipherPrivateKey: conf.CipherPrivateKey,
			},
		}
		if cryptoKit, err = kit.New(types.CipherScheme_ServiceBasedKMS, configure); err != errno.OK {
			return
		}
	})
	return fileError(err)
}

// Init init server client of crypto kit.
func Init(conf Configure) error {
	return initOnce(conf)
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
		block:   pool.Get().([]byte)[:cacheSize],
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
		block:   pool.Get().([]byte)[:cacheSize],
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
				r.free()
				if n > 0 {
					return n, nil
				}
				return n, r.err
			}
		}

		read := copy(p, r.block[r.offset:])
		r.offset += read

		p = p[read:]
		n += read
	}

	sizeBlock := len(r.block)
	if r.offset == sizeBlock {
		if sizeBlock < cacheSize { // EOF
			r.free()
			r.err = io.EOF
		} else {
			if r.err = r.nextBlock(); r.err != nil {
				r.free()
			}
		}
	}

	return n, nil
}

func (r *fileCryptor) nextBlock() error {
	n, err := readFullOrToEnd(r.reader, r.block)
	r.offset = 0
	r.block = r.block[:n]

	start := 0
	for start < n {
		end := start + int(BlockSize)
		if end > n {
			end = n
		}
		block := r.block[start:end]
		if eno := r.cryptor(block, block, 0); eno != errno.OK {
			return fileError(eno)
		}
		start += int(BlockSize)
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
