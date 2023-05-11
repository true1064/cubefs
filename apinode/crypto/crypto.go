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
	"io"

	_ "oppo.com/andes-crypto/kit/server"
)

// EnvironmentType for describing the usage environment.
type EnvironmentType uint16

// Environments
const (
	EnvironmentDevelop EnvironmentType = 1
	EnvironmentTest    EnvironmentType = 2
	EnvironmentProduct EnvironmentType = 3
)

// CipherScheme cipher level.
type CipherScheme uint16

// CipherSchemes
const (
	CipherSchemeCommon  CipherScheme = 1
	CipherSchemeService CipherScheme = 2
	CipherSchemeUser    CipherScheme = 3
	CipherSchemeDevice  CipherScheme = 4
)

// TransCipher cipher for transmitting.
type TransCipher interface {
	Encrypt(plaintext []byte) (ciphertext []byte, err error)
	Decrypt(ciphertext []byte) (plaintext []byte, err error)
	Encryptor(plaintexter io.Reader) (ciphertexter io.Reader)
	Decryptor(ciphertexter io.Reader) (plaintexter io.Reader)
}

// FileCipher cipher for file content.
type FileCipher interface {
	Key() []byte
	Encrypt(plaintext []byte) (ciphertext []byte, err error)
	Decrypt(ciphertext []byte) (plaintext []byte, err error)
	Encryptor(plaintexter io.Reader) (ciphertexter io.Reader)
	Decryptor(ciphertexter io.Reader) (plaintexter io.Reader)
}

// Init initializes crypto kit once.
func Init(psa string, env EnvironmentType) error {
	return nil
}

// GetTransCipher returns the transfer encryption and decryption object.
func GetTransCipher(ticket []byte) (TransCipher, error) {
	return nil, nil
}

// GetFileCipher returns the file encryption and decryption object.
func GetFileCipher(key []byte) (FileCipher, error) {
	return nil, nil
}
