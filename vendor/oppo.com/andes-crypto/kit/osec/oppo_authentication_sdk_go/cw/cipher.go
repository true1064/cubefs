package cw

import (
	"bytes"
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"hash"
	"io"
	"math"
	"math/big"
	"reflect"
	"strings"

	"github.com/astaxie/beego/logs"
	"github.com/pkg/errors"
	"go.mozilla.org/pkcs7"
)

// PEM TYPE
const (
	PemTypePkcs8Priv = "PRIVATE KEY"
	PemTypePkixPub   = "PUBLIC KEY"

	PemTypeEcPub  = "EC PUBLIC KEY"
	PemTypeEcPriv = "EC PRIVATE KEY"

	PemTypePkcs1Priv = "RSA PRIVATE KEY"
	PemTypePkc1Pub   = "RSA PUBLIC KEY"

	PemTypeCertReq = "CERTIFICATE REQUEST"
	PemTypeCert    = "CERTIFICATE"
)

// ECC group name
const (
	NISTP256 = "NIST-P256"
	NISTP384 = "NIST-P384"
	NISTP521 = "NIST-P521"
)

// HMAC256 hash-based message authentication code
func HMAC256(key []byte, message ...[]byte) ([]byte, error) {
	if len(key) <= 0 {
		err := errors.New("input key invalid")
		return nil, err
	}
	md := hmac.New(sha256.New, key)

	for _, v := range message {
		md.Write(v)
	}

	return md.Sum(nil), nil
}

// GetX509FromPem GetX509FromPem
func GetX509FromPem(in []byte) (cert *x509.Certificate, err error) {
	block, _ := pem.Decode(in)
	if block == nil {
		return nil, errors.New("pem.Decode ceritificate be")
	}

	cert, err = x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}

	return cert, nil
}

// P7Decrypt Pkcs7Decrypt
func P7Decrypt(PemCert []byte,
	pemEPrivateKey []byte,
	cipher []byte) (plain []byte, err error,
) {
	block, _ := pem.Decode(pemEPrivateKey)
	if block == nil {
		return nil, errors.New("input pem be")
	}
	// 解析PKCS1格式的私钥
	priv, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	if err != nil {
		err = errors.Wrap(err, "private key")
		return nil, err
	}

	cert, err := GetX509FromPem(PemCert)
	if err != nil {
		err = errors.Wrap(err, "certificate")
		return nil, err
	}

	p7, err := pkcs7.Parse(cipher)
	if err != nil {
		err = errors.Wrap(err, "")
		return nil, err
	}

	plain, err = p7.Decrypt(cert, priv)
	if err != nil {
		err = errors.Wrap(err, "")
		return nil, err
	}

	return plain, nil
}

// P7Encrypt Pkcs7Decrypt
func P7Encrypt(PemCert []byte, in []byte) (out []byte, err error) {
	cert, err := GetX509FromPem(PemCert)
	if err != nil {
		err = errors.Wrap(err, "certificate")
		return nil, err
	}

	out, err = pkcs7.Encrypt(in, []*x509.Certificate{cert})
	if err != nil {
		err = errors.Wrap(err, "")
		return nil, err
	}

	return out, nil
}

// GenerateRand generates random number
func GenerateRand(bitLength int) (rn []byte, err error) {
	if bitLength%8 != 0 {
		err = errors.Errorf("bitLength should multiple of 8 bits")
		return nil, err
	}

	rn = make([]byte, bitLength/8)
	_, err = rand.Read(rn)
	if nil != err {
		return nil, err
	}

	return rn, err
}

// AESGCMEncrypt AESGCMEncrypt
func AESGCMEncrypt(key []byte,
	plain []byte,
	nonce []byte,
	aad []byte) (encrypted []byte, err error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	encrypted = gcm.Seal(nil, nonce, plain, aad)

	return encrypted, nil
}

// AESGCMDecrypt AESGCMDecrypt
func AESGCMDecrypt(key []byte,
	encrypted []byte,
	nonce []byte,
	aad []byte) (plain []byte, err error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	plain, err = gcm.Open(nil, nonce, encrypted, aad)
	if err != nil {
		return nil, err
	}

	return plain, nil
}

// PackAES a struct for packing gcm result
type PackAES struct {
	IvLen  int    `json:"IvLen"`  // nonce length in bytes
	Iv     string `json:"Iv"`     // base64 encoded nonce
	Cipher string `json:"Cipher"` // base64 encoded ciphertext
}

// AESCBCPackEncrypt AESCBCPackEncrypt
func AESCBCPackEncrypt(key []byte, plain []byte) (pack []byte, err error) {
	p := PackAES{}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	plain = PKCS7Padding(plain, aes.BlockSize)

	encrypted := make([]byte, len(plain))
	nonce := make([]byte, aes.BlockSize)
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	// fmt.Printf("Nonce: %X\n", nonce)
	// fmt.Printf("plain: %X\n", plain)
	p.IvLen = aes.BlockSize
	p.Iv = base64.StdEncoding.EncodeToString(nonce)

	cbc := cipher.NewCBCEncrypter(block, nonce)
	cbc.CryptBlocks(encrypted, plain)
	p.Cipher = base64.StdEncoding.EncodeToString(encrypted)

	pack, err = json.Marshal(p)
	if err != nil {
		return nil, err
	}
	return pack, nil
}

// AESCBCPackDecrypt decrypt pack outputted by AESCBCPackEncrypt
func AESCBCPackDecrypt(key []byte, pack []byte) (plain []byte, err error) {
	p := PackAES{}

	err = json.Unmarshal(pack, &p)
	if err != nil {
		return nil, err
	}
	cipherblob, err := base64.StdEncoding.DecodeString(p.Cipher)
	if err != nil {
		return nil, err
	}

	nonce, err := base64.StdEncoding.DecodeString(p.Iv)

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// fmt.Printf("Nonce: %X\n", nonce)
	// fmt.Printf("Cipher: %X\n", cipherblob)

	plain = make([]byte, len(cipherblob))
	cbc := cipher.NewCBCDecrypter(block, nonce)
	cbc.CryptBlocks(plain, cipherblob)
	plain = PKCS7UnPadding(plain)

	return plain, nil
}

// AESGCMPackEncrypt encrypt with gcm mode
func AESGCMPackEncrypt(key []byte, plain []byte) (pack []byte, err error) {
	p := PackAES{}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	p.IvLen = gcm.NonceSize()
	nonce := make([]byte, p.IvLen)
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	////fmt.Printf("Nonce: %X\n", nonce)
	////fmt.Printf("plain: %X\n", plain)
	p.Iv = base64.StdEncoding.EncodeToString(nonce)

	ciphertext := gcm.Seal(nil, nonce, plain, nil)
	////fmt.Printf("cipher: %X\n", ciphertext)
	p.Cipher = base64.StdEncoding.EncodeToString(ciphertext)

	pack, err = json.Marshal(p)
	if err != nil {
		return nil, err
	}

	return pack, nil
}

// AESGCMPackDecrypt decrypt pack outputted by AESGCMPackEncrypt gcm mode
func AESGCMPackDecrypt(key []byte, pack []byte) (plain []byte, err error) {
	p := PackAES{}

	err = json.Unmarshal(pack, &p)
	if err != nil {
		return nil, err
	}
	cipherblob, err := base64.StdEncoding.DecodeString(p.Cipher)
	if err != nil {
		return nil, err
	}

	nonce, err := base64.StdEncoding.DecodeString(p.Iv)
	if err != nil {
		return nil, err
	}

	plain, err = AESGCMDecrypt(key, cipherblob, nonce, nil)
	if err != nil {
		return nil, err
	}

	return plain, nil
}

// AESCTRPackEncrypt encrypt with ctr mode
func AESCTRPackEncrypt(key []byte, plain []byte) (pack []byte, err error) {
	p := PackAES{}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	ciphertext := make([]byte, aes.BlockSize+len(plain))
	iv := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}

	stream := cipher.NewCTR(block, iv)
	stream.XORKeyStream(ciphertext, plain)

	p.IvLen = len(iv)

	p.Iv = base64.StdEncoding.EncodeToString(iv)
	p.Cipher = base64.StdEncoding.EncodeToString(ciphertext[:len(plain)])

	pack, err = json.Marshal(p)
	if err != nil {
		return nil, err
	}

	return pack, nil
}

// AESCTRPackDecrypt decrypt pack outputted by AESCTRPackEncrypt ctr mode
func AESCTRPackDecrypt(key []byte, pack []byte) (plain []byte, err error) {
	p := PackAES{}

	err = json.Unmarshal(pack, &p)
	if err != nil {
		return nil, err
	}
	cipherblob, err := base64.StdEncoding.DecodeString(p.Cipher)
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	iv, err := base64.StdEncoding.DecodeString(p.Iv)
	if err != nil {
		return nil, err
	}
	plain = make([]byte, len(cipherblob))
	stream := cipher.NewCTR(block, iv)
	stream.XORKeyStream(plain, cipherblob)

	return plain, nil
}

// DER2PEMEPrivate translate DER coded PKCS8 format pravate key into PEM coding
func DER2PEMEPrivate(derPrivate []byte) (pemEPrivate []byte) {
	blockPub := &pem.Block{
		Type:  PemTypePkcs8Priv,
		Bytes: derPrivate,
	}

	return pem.EncodeToMemory(blockPub)
}

// DER2PEMPublic translate DER coded PKCS8 format public key into PEM coding
func DER2PEMPublic(derPub []byte) (pemPub []byte) {
	blockPub := &pem.Block{
		Type:  PemTypePkixPub,
		Bytes: derPub,
	}

	return pem.EncodeToMemory(blockPub)
}

// Asn12Pem 将ASN.1转PEM格式
func Asn12Pem(asn1Data []byte, pemType string) []byte {
	blockPub := &pem.Block{
		Type:  pemType,
		Bytes: asn1Data,
	}

	return pem.EncodeToMemory(blockPub)
}

// Pem2Asn1 将PEM转ASN.1格式
func Pem2Asn1(pemData []byte, pemType string) ([]byte, error) {
	var err error

	block, _ := pem.Decode(pemData)
	if block == nil {
		err = errors.Errorf("Decode pem private key failed.")
		return nil, err
	}

	if strings.Compare(block.Type, pemType) != 0 {
		err = errors.Errorf("PEM Data type failed, input %s, need %s", block.Type, pemType)
		return nil, err
	}

	return block.Bytes, nil
}

// Pem2x509CSR PEM to Internal object, x509 certificate signing request
func Pem2x509CSR(pem []byte) (*x509.CertificateRequest, error) {
	// 1. parse and check input CSR
	asn1, err := Pem2Asn1(pem, PemTypeCertReq)
	if err != nil {
		logs.Error("%v", err)
		err = errors.New("trans Pem CSR to asn1 failed")
		logs.Error("%v", err)
		return nil, err
	}

	csr, err := x509.ParseCertificateRequest(asn1)
	if err != nil {
		logs.Error("%v", err)
		err = errors.New("x509.ParseCertificateRequest failed")
		return nil, err
	}

	return csr, nil
}

// Pem2x509Cert PEM to Internal object, x509 certificate
func Pem2x509Cert(pem []byte) (*x509.Certificate, error) {
	// 1. parse and check input CSR
	asn1, err := Pem2Asn1(pem, PemTypeCert)
	if err != nil {
		logs.Error("%v", err)
		err = errors.New("trans Pem CERT to asn1 failed")
		logs.Error("%v", err)
		return nil, err
	}

	cert, err := x509.ParseCertificate(asn1)
	if err != nil {
		logs.Error("%v", err)
		err = errors.New("x509.ParseCertificate failed")
		return nil, err
	}

	return cert, nil
}

// PEM2DERPrivate translate PEM coded PKCS#8 format private key into DER coding
func PEM2DERPrivate(pemEPrivate []byte) (derPrivate []byte, err error) {
	block, _ := pem.Decode(pemEPrivate)
	if block == nil {
		err = errors.Errorf("Decode pem private key failed.")
		return nil, err
	}

	if strings.Compare(block.Type, PemTypePkcs8Priv) != 0 {
		err = errors.Errorf("not support block type:%v", block.Type)
		return nil, err
	}

	return block.Bytes, nil
}

// PEM2DERPublic translate PEM coded PKCS#8 format public key into DER coding
func PEM2DERPublic(pemPublic []byte) (derPrivate []byte, err error) {
	block, _ := pem.Decode(pemPublic)
	if block == nil {
		err = errors.Errorf("Decode pem public key failed.")
		return nil, err
	}

	if strings.Compare(block.Type, PemTypePkixPub) != 0 {
		err = errors.Errorf("not support block type:%v", block.Type)
		return nil, err
	}

	return block.Bytes, nil
}

// RSAGenerate RSAGenerate
func RSAGenerate(bits int) (pemPublic []byte, pemEPrivate []byte, err error) {
	priv, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, nil, err
	}

	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, nil, err
	}

	block := &pem.Block{
		Type:  PemTypePkcs8Priv,
		Bytes: der,
	}
	pemEPrivate = pem.EncodeToMemory(block)

	derPub, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		return nil, nil, err
	}

	blockPub := &pem.Block{
		Type:  PemTypePkixPub,
		Bytes: derPub,
	}

	pemPublic = pem.EncodeToMemory(blockPub)

	return pemPublic, pemEPrivate, nil
}

// ECCGenerate RSAGenerate
func ECCGenerate(groupName string) (pemPublic []byte, pemPrivate []byte, err error) {
	var c elliptic.Curve

	if strings.ToLower(NISTP256) == strings.ToLower(groupName) {
		c = elliptic.P256()
	} else if strings.ToLower(NISTP384) == strings.ToLower(groupName) {
		c = elliptic.P384()
	} else if strings.ToLower(NISTP521) == strings.ToLower(groupName) {
		c = elliptic.P521()
	} else {
		err = errors.Errorf("Wrong ecc group name(%v), only support(%v, %v, %v).",
			groupName, NISTP256, NISTP384, NISTP521)
		return nil, nil, err
	}

	priv, err := ecdsa.GenerateKey(c, rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, nil, err
	}

	block := &pem.Block{
		Type:  PemTypePkcs8Priv,
		Bytes: der,
	}
	pemPrivate = pem.EncodeToMemory(block)

	derPub, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		return nil, nil, err
	}

	blockPub := &pem.Block{
		Type:  PemTypePkixPub,
		Bytes: derPub,
	}

	pemPublic = pem.EncodeToMemory(blockPub)

	return pemPublic, pemPrivate, nil
}

// ParsePemPrivateKey parse pivate key
func ParsePemPrivateKey(pemPriv []byte) (key interface{}, err error) {
	block, _ := pem.Decode(pemPriv)
	if block == nil {
		err = errors.Errorf("decode pem private key failed.")
		return nil, err
	}

	if block.Type == "RSA PRIVATE KEY" {
		key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			err = errors.Wrap(err, "x509.ParsePKCS1PrivateKey(block.Bytes)")
			return nil, err
		}

		return key, nil

	} else if block.Type == "PRIVATE KEY" {
		key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, err
		}

		return key, nil
	}

	err = errors.Errorf("not support block type:%v", block.Type)
	return nil, err
}

// ParsePemEccPrivateKey parse a PKCS8 encode PEM private key into *ecdsa.Private
func ParsePemEccPrivateKey(pemPriv []byte) (key *ecdsa.PrivateKey, err error) {
	block, _ := pem.Decode(pemPriv)
	if block == nil {
		err = errors.Errorf("decode pem private key failed.")
		return nil, err
	}

	if block.Type == "PRIVATE KEY" {
		priv, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, err
		}

		key, ok := priv.(*ecdsa.PrivateKey)
		if !ok {
			err = errors.Errorf("Wrong type, this is a %v", reflect.TypeOf(priv).Kind())
			return nil, err
		}
		return key, nil
	}

	err = errors.Errorf("not support block type:%v", block.Type)
	return nil, err
}

// ParsePemEccPublicKey parse a PKCS8 encode PEM public key into *ecdsa.PublicKey
func ParsePemEccPublicKey(pemPub []byte) (pub *ecdsa.PublicKey, err error) {
	block, _ := pem.Decode(pemPub)
	if block == nil {
		err = errors.Errorf("decode pem public key failed.")
		return nil, err
	}

	if block.Type == "PUBLIC KEY" {
		re, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			err = errors.Wrap(err, "ParsePKIXPublicKey block.Bytes")
			return nil, err
		}

		repub, ok := re.(*ecdsa.PublicKey)
		if !ok {
			err = errors.Errorf("not supported kind:%v", reflect.ValueOf(re).Kind())
			return nil, err
		}

		return repub, nil
	}

	err = errors.Errorf("not support block type:%v", block.Type)
	return nil, err
}

// ParsePemRsaPrivateKey parse rsa key
func ParsePemRsaPrivateKey(pemPriv []byte) (key *rsa.PrivateKey, err error) {
	block, _ := pem.Decode(pemPriv)
	if block == nil {
		err = errors.Errorf("decode pem private key failed.")
		return nil, err
	}

	if block.Type == "RSA PRIVATE KEY" {
		key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			err = errors.Wrap(err, "x509.ParsePKCS1PrivateKey(block.Bytes)")
			return nil, err
		}

		return key, nil

	} else if block.Type == "PRIVATE KEY" {
		priv, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, err
		}

		key, ok := priv.(*rsa.PrivateKey)
		if !ok {
			err = errors.Errorf("Wrong type, not \"*rsa.PrivateKey\"")
			return nil, err
		}

		return key, nil
	}

	err = errors.Errorf("not support block type:%v", block.Type)
	return nil, err
}

// ParsePemRsaPublicKey parse a PKCS8 or PKCS1 encode PEM public key into *rsa.PublicKey
func ParsePemRsaPublicKey(pemPub []byte) (pub *rsa.PublicKey, err error) {
	block, _ := pem.Decode(pemPub)
	if block == nil {
		err = errors.Errorf("decode pem public key failed.")
		return nil, err
	}

	if block.Type == "PUBLIC KEY" {

		re, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			err = errors.Wrap(err, "ParsePKIXPublicKey block.Bytes")
			return nil, err
		}

		repub, ok := re.(*rsa.PublicKey)
		if !ok {
			err = errors.Errorf("Wrong type, not \"*rsa.PublicKey\"")
			return nil, err
		}

		return repub, nil

	} else if block.Type == "RSA PUBLIC KEY" {

		pub, err := x509.ParsePKCS1PublicKey(block.Bytes)
		if err != nil {
			err = errors.Wrap(err, "ParsePKCS1PublicKey block.Bytes")
			return nil, err
		}

		return pub, nil
	}

	err = errors.Errorf("not support block type:%v", block.Type)
	return nil, err
}

// RSASignPKCS1v15 RSA Sign PKCS1v15
func RSASignPKCS1v15(priv []byte,
	data []byte,
	isHash bool,
	hashMethod string) (signature []byte, err error) {
	privKey, err := ParsePemRsaPrivateKey(priv)
	if err != nil {
		err = errors.Wrap(err, "ParsePemRsaPrivateKey")
		return nil, err
	}

	var hs hash.Hash
	var paramHs crypto.Hash
	var md []byte

	if isHash {
		switch hashMethod {
		case "SHA384":
			hs = sha512.New384()
			paramHs = crypto.SHA384
		case "SHA512":
			hs = sha512.New()
			paramHs = crypto.SHA512
		default:
			hs = sha256.New()
			paramHs = crypto.SHA256
		}
		hs.Write(data)
		md = hs.Sum(nil)
	} else {
		paramHs = 0
		md = data
	}

	signature, err = rsa.SignPKCS1v15(rand.Reader, privKey, paramHs, md[:])
	return signature, err
}

// RSAVerifyPKCS1v15 RSA Verify PKCS1v15
func RSAVerifyPKCS1v15(pub []byte,
	data []byte,
	signature []byte,
	isHash bool,
	hashMethod string) (ok bool, err error) {
	pubKey, err := ParsePemRsaPublicKey(pub)
	if err != nil {
		err = errors.Wrap(err, "RSAParsePEMPub")
		return false, err
	}

	var hs hash.Hash
	var paramHs crypto.Hash
	var md []byte

	if isHash {
		switch hashMethod {
		case "SHA384":
			hs = sha512.New384()
			paramHs = crypto.SHA384
		case "SHA512":
			hs = sha512.New()
			paramHs = crypto.SHA512
		default:
			hs = sha256.New()
			paramHs = crypto.SHA256
		}
		hs.Write(data)
		md = hs.Sum(nil)
	} else {
		paramHs = 0
		md = data
	}

	err = rsa.VerifyPKCS1v15(pubKey, paramHs, md[:], signature)
	if err != nil {
		return false, err
	}

	return true, nil
}

// RSASignPSS RSA Sign PSS
func RSASignPSS(priv []byte,
	data []byte,
	hashMethod string,
	sLen int) (signature []byte, err error) {
	privKey, err := ParsePemRsaPrivateKey(priv)
	if err != nil {
		err = errors.Wrap(err, "ParsePemRsaPrivateKey")
		return nil, err
	}

	var hs hash.Hash
	var paramHs crypto.Hash
	var md []byte

	switch hashMethod {
	case "SHA384":
		hs = sha512.New384()
		paramHs = crypto.SHA384
	case "SHA512":
		hs = sha512.New()
		paramHs = crypto.SHA512
	default:
		hs = sha256.New()
		paramHs = crypto.SHA256
	}

	hs.Write(data)
	md = hs.Sum(nil)

	var opts *rsa.PSSOptions
	if sLen != 0 {
		temp := rsa.PSSOptions{SaltLength: sLen}
		opts = &temp
	}

	signature, err = rsa.SignPSS(rand.Reader, privKey, paramHs, md[:], opts)
	return signature, err
}

// RSAVerifyPSS RSA Verify PSS
func RSAVerifyPSS(pub []byte,
	data []byte,
	signature []byte,
	hashMethod string,
	sLen int) (ok bool, err error) {
	pubKey, err := ParsePemRsaPublicKey(pub)
	if err != nil {
		err = errors.Wrap(err, "RSAParsePEMPub")
		return false, err
	}

	var hs hash.Hash
	var paramHs crypto.Hash
	var md []byte

	switch hashMethod {
	case "SHA384":
		hs = sha512.New384()
		paramHs = crypto.SHA384
	case "SHA512":
		hs = sha512.New()
		paramHs = crypto.SHA512
	default:
		hs = sha256.New()
		paramHs = crypto.SHA256
	}

	hs.Write(data)
	md = hs.Sum(nil)

	var opts *rsa.PSSOptions
	if sLen != 0 {
		temp := rsa.PSSOptions{SaltLength: sLen}
		opts = &temp
	}

	err = rsa.VerifyPSS(pubKey, paramHs, md[:], signature, opts)
	if err != nil {
		return false, err
	}

	return true, nil
}

// ECCSign ECC Sign
func ECCSign(priv []byte,
	data []byte,
	isHash bool,
	hashMethod string) (R []byte, S []byte, err error) {
	privKey, err := ParsePemEccPrivateKey(priv)
	if err != nil {
		err = errors.Wrap(err, "ParsePemEccPrivateKey")
		return nil, nil, err
	}

	var hs hash.Hash
	var md []byte

	if isHash {
		switch hashMethod {
		case "SHA384":
			hs = sha512.New384()
		case "SHA512":
			hs = sha512.New()
		default:
			hs = sha256.New()
		}
		hs.Write(data)
		md = hs.Sum(nil)
	} else {
		md = data
	}

	r, s, err := ecdsa.Sign(rand.Reader, privKey, md[:])
	if err != nil {
		return nil, nil, err
	}

	rtext, err := r.MarshalText()
	if err != nil {
		err = errors.Wrap(err, "R MarshalText")
		return nil, nil, err
	}

	stext, err := s.MarshalText()
	if err != nil {
		err = errors.Wrap(err, "S MarshalText")
		return nil, nil, err
	}

	return rtext, stext, nil
}

// ECCVerify ECC verify
func ECCVerify(pub []byte,
	data []byte,
	isHash bool,
	hashMethod string,
	R []byte,
	S []byte) (verify bool, err error) {
	pubKey, err := ParsePemEccPublicKey(pub)
	if err != nil {
		err = errors.Wrap(err, "ParsePemEccPublicKey")
		return false, err
	}

	var hs hash.Hash
	var md []byte

	if isHash {
		switch hashMethod {
		case "SHA384":
			hs = sha512.New384()
		case "SHA512":
			hs = sha512.New()
		default:
			hs = sha256.New()
		}
		hs.Write(data)
		md = hs.Sum(nil)
	} else {
		md = data
	}

	var r, s big.Int
	err = r.UnmarshalText(R)
	if err != nil {
		err = errors.Wrap(err, "UnmarshalText R")
		return false, err
	}
	err = s.UnmarshalText(S)
	if err != nil {
		err = errors.Wrap(err, "UnmarshalText S")
		return false, err
	}

	verify = ecdsa.Verify(pubKey, md[:], &r, &s)
	return verify, nil
}

// RSAVerifyPKCS1v15withSha256 use RSASSA-PKCS1-v1_5 to verify.
//    use HSA256 to cacl md of data
func RSAVerifyPKCS1v15withSha256(PEMPub []byte, data []byte, signature []byte) error {
	pub, err := ParsePemRsaPublicKey(PEMPub)
	if err != nil {
		return err
	}

	md := sha256.Sum256(data)

	err = rsa.VerifyPKCS1v15(pub, crypto.SHA256, md[:], signature)
	if err != nil {
		return err
	}

	return nil
}

// RSASignPKCS1v15withSha256 RSA Sign PKCS1v15 By Sha256
func RSASignPKCS1v15withSha256(PEMPriv []byte, data []byte) (signature []byte, err error) {
	priv, err := ParsePemRsaPrivateKey(PEMPriv)
	if err != nil {
		err = errors.Wrap(err, "ParsePemRsaPrivateKey")
		return nil, err
	}

	md := sha256.Sum256(data)

	signature, err = rsa.SignPKCS1v15(rand.Reader, priv, crypto.SHA256, md[:])
	return signature, err
}

// RSAVerifyPSSwithSha256 use PKCS 1 PSS to verify.
func RSAVerifyPSSwithSha256(PEMPub []byte, data []byte, signature []byte) error {
	pub, err := ParsePemRsaPublicKey(PEMPub)
	if err != nil {
		return err
	}

	md := sha256.Sum256(data)

	opts := rsa.PSSOptions{SaltLength: 32}

	err = rsa.VerifyPSS(pub, crypto.SHA256, md[:], signature, &opts)
	if err != nil {
		return err
	}

	return nil
}

// RSASignPSSwithSha256 RSA Sign PKCS1 PSS By Sha256
func RSASignPSSwithSha256(PEMPriv []byte, data []byte) (signature []byte, err error) {
	priv, err := ParsePemRsaPrivateKey(PEMPriv)
	if err != nil {
		err = errors.Wrap(err, "ParsePemRsaPrivateKey")
		return nil, err
	}

	md := sha256.Sum256(data)
	opts := rsa.PSSOptions{SaltLength: 32}

	signature, err = rsa.SignPSS(rand.Reader, priv, crypto.SHA256, md[:], &opts)
	return signature, err
}

// RSAEncryptPKCS1OAEP RSA Encrypt PKCS1 OAEP
func RSAEncryptPKCS1OAEP(PEMPub []byte, data []byte) (encrypted []byte, err error) {
	pub, err := ParsePemRsaPublicKey(PEMPub)
	if err != nil {
		return nil, err
	}

	// if len(data) >= (pub.Size() - 2*sha256.Size - 2) {
	if len(data) >= (pub.Size() - 2*sha1.Size - 2) {
		err = errors.Errorf("data too large, len:%v", len(data))
		return nil, err
	}

	// return rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, data, nil)
	return rsa.EncryptOAEP(sha1.New(), rand.Reader, pub, data, nil)
}

// RSAEncryptPKCS1v15 RSA Encrypt PKCS1 v15
func RSAEncryptPKCS1v15(PEMPub []byte, data []byte) (encrypted []byte, err error) {
	pub, err := ParsePemRsaPublicKey(PEMPub)
	if err != nil {
		return nil, err
	}

	if len(data) >= (pub.Size() - 2*sha256.Size - 2) {
		err = errors.Errorf("data too large, len:%v", len(data))
		return nil, err
	}

	return rsa.EncryptPKCS1v15(rand.Reader, pub, data)
}

// RSADecryptPKCS1OAEP decrypt oaep
func RSADecryptPKCS1OAEP(PEMPriv []byte, ciphertext []byte) (plaintext []byte, err error) {
	priv, err := ParsePemRsaPrivateKey(PEMPriv)
	if err != nil {
		err = errors.Wrap(err, "ParsePemRsaPrivateKey")
		return nil, err
	}

	// return rsa.DecryptOAEP(sha256.New(), rand.Reader, priv, ciphertext, nil)
	return rsa.DecryptOAEP(sha1.New(), rand.Reader, priv, ciphertext, nil)
}

// RSADecryptPKCS1v15 decrypt pkcs1 v1.5
func RSADecryptPKCS1v15(PEMPriv []byte, ciphertext []byte) (plaintext []byte, err error) {
	priv, err := ParsePemRsaPrivateKey(PEMPriv)
	if err != nil {
		err = errors.Wrap(err, "ParsePemRsaPrivateKey")
		return nil, err
	}

	return rsa.DecryptPKCS1v15(rand.Reader, priv, ciphertext)
}

// PackRSAEnvelope a package for storing the rsa envelop data
type PackRSAEnvelope struct {
	AlgMode     string `json:"AlgMode"`     // AES-256-GCM, AES-256-CBC
	PaddingMode int    `json:"PaddingMode"` // PKCS7
	EDEK        []byte `json:"EDEK"`        // RSAES-OAEP encrypted dek
	Blob        []byte `json:"Blob"`        // aes gcm encrypted data blob
}

// PKCS7Padding add PKCS7 Padding
func PKCS7Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

// PKCS7UnPadding remove pkcs 7 padding
func PKCS7UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

//
//// DeriveUserKeyStep2 派生UserKey需要两次派生，这是第二次派生
//func DeriveUserKeyStep2(firstmd []byte) []byte {
//
//	var md [32]byte
//	var tb []byte
//
//	for idx := 0; idx < 10; idx++ {
//		tb = append(tb, []byte(strconv.Itoa(idx))...)
//		tb = append(tb, []byte(firstmd)...)
//		tb = append(tb, md[:]...)
//
//		md = sha256.Sum256(tb)
//	}
//
//	return md[:]
//}
//
//// DeriveUserKeyStep1 派生UserKey需要两次派生，第一次派生
//func DeriveUserKeyStep1(username, password string) (firstmd []byte) {
//	in := username + ".oppp." + password
//
//	md := sha256.Sum256([]byte(in))
//
//	return md[:]
//}

// KdfNistCtr NIST SP 800-108 ctr mode kdf use SHA-256 as HMAC.
//The input is:
//
// Key:
//    256-bit HMAC secret.
// Label:
//    Null-terminated ASCII string, encoded as bytes, including the null-terminator.
// Context:
//    Optional binary string containing information related to the derived keying material.
// bitsLen:
//    An unsigned integer specifying the length (in bits) of the output keying material.
//    Encoded as a big-endian, 32-bit unsigned integer. This encoding limits the maximum
//    output bit-count to MAX_UINT32.
// i:
//    The counter, encoded as a big-endian, 32-bit unsigned integer.
//
// Process:
// 1. If (L > MAX_UINT32), then indicate an be and stop
// 2. n <- ceil(L/256) // 256 is the digest length for SHA256
// 3. result(0) <- {}
// 4. For i from 1 to n:
//   a. K(i) <- HMAC_SH256(Key, i || Label || Context || L)
//   b. result(i) <- result(i-1) || K(i)
// 5. Return the leftmost L bits of result(n)
func KdfNistCtr(key []byte, label string, context []byte, bitsLen uint32) (result []byte, err error) {
	// 1. check input parameters
	if bitsLen%8 != 0 {
		return nil, errors.New("input bits length MUST multiples of 8")
	}

	// 2. 以Null结尾的字符串
	labelNull := []byte(label)
	labelNull = append(labelNull, 0)

	// 3. 输出比特长度，大端表示
	lenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBytes, uint32(bitsLen))

	// 4. 迭代次数，大端表示
	roundBytes := make([]byte, 4)

	// 5. 总循环迭代次数
	n := int(math.Ceil(float64(bitsLen) / float64(256)))
	for i := 1; i <= n; i++ {
		binary.BigEndian.PutUint32(roundBytes, uint32(i))
		md, err := HMAC256(key, roundBytes, labelNull, context, lenBytes)
		if err != nil {
			return nil, err
		}

		appendCount := int(bitsLen)/8 - len(result)
		if appendCount > 32 {
			appendCount = 32
		}
		result = append(result, md[:appendCount]...)
	}

	return result, nil
}
