package cw

import (
	"crypto/x509"
	"errors"

	"github.com/astaxie/beego/logs"
)

//  KeyUsageDigitalSignature KeyUsage = 1 << iota
//	KeyUsageContentCommitment
//	KeyUsageKeyEncipherment
//	KeyUsageDataEncipherment
//	KeyUsageKeyAgreement
//	KeyUsageCertSign
//	KeyUsageCRLSign
//	KeyUsageEncipherOnly
//	KeyUsageDecipherOnly
var keyUsageNames = []struct {
	usage x509.KeyUsage
	name  string
}{
	{x509.KeyUsageDigitalSignature, "DigitalSignature"},
	{x509.KeyUsageContentCommitment, "ContentCommitment"},
	{x509.KeyUsageKeyEncipherment, "KeyEncipherment"},
	{x509.KeyUsageDataEncipherment, "DataEncipherment"},
	{x509.KeyUsageKeyAgreement, "KeyAgreement"},
	{x509.KeyUsageCertSign, "CertSign"},
	{x509.KeyUsageCRLSign, "CRLSign"},
	{x509.KeyUsageEncipherOnly, "EncipherOnly"},
	{x509.KeyUsageDecipherOnly, "DecipherOnly"},
}

// KeyUsage2String KeyUsage2String
func KeyUsage2String(keyUsage x509.KeyUsage) string {
	var str string = ""
	for _, v := range keyUsageNames {
		if v.usage&keyUsage != 0 {
			str += " " + v.name
		}
	}

	return str
}

// TryParsePemCertificate TryParsePemCertificate
func TryParsePemCertificate(pemCertificate []byte) bool {
	if len(pemCertificate) == 0 {
		logs.Info("Input invalid")
		return false
	}

	parentAsn1Cert, err := Pem2Asn1(pemCertificate, "CERTIFICATE")
	if err != nil {
		logs.Info("%v", err)
		return false
	}

	// 2.2. Parse parent certificate
	_, err = x509.ParseCertificate(parentAsn1Cert)
	if err != nil {
		logs.Info("%v", err)
		return false
	}

	return true
}

// CertChainVerifyCert CertChainVerifyCert
func CertChainVerifyCert(pemCertToBeVerify string, rootCertPem ...string) bool {
	var err error

	roots := x509.NewCertPool()

	for _, v := range rootCertPem {
		ok := roots.AppendCertsFromPEM([]byte(v))
		if !ok {
			err = errors.New("failed to parse root certificate")
			logs.Error("%v", err)
			return false
		}
	}

	opts := x509.VerifyOptions{
		//	DNSName: "mail.google.com",
		Roots: roots,
	}

	asn1, err := Pem2Asn1([]byte(pemCertToBeVerify), PemTypeCert)
	if err != nil {
		logs.Error("%v", err)
		return false
	}

	// 2.2. Parse parent certificate
	cert, err := x509.ParseCertificate(asn1)
	if err != nil {
		logs.Error("%v", err)
		return false
	}

	_, err = cert.Verify(opts)
	if err != nil {
		logs.Error("%v", err)
		return false
	}

	return true
}
