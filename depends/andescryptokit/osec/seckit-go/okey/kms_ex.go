package okey

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"

	cmap "github.com/orcaman/concurrent-map"
	"github.com/pkg/errors"
	"github.com/robfig/cron"

	kmserr "andescryptokit/osec/seckit-go/okey/error"
	"andescryptokit/osec/seckit-go/okey/model"
)

type KmsEx struct {
	AppName        string
	Ak             string
	Sk             string
	KMS_CIPHER_MAP cmap.ConcurrentMap
	KMS_URL_MAP    map[int]string
	AUTH_URL_MAP   map[int]string
	KMSTransObj    *KmsTransmission
	EnvCode        int
}

var (
	GKmsEx *KmsEx
	lock   sync.Mutex
	IsInit bool
)

func InitMap() {
	GKmsEx.KMS_CIPHER_MAP = cmap.New()

	GKmsEx.KMS_URL_MAP = make(map[int]string)
	GKmsEx.AUTH_URL_MAP = make(map[int]string)

	KMS_URL_MAP[DEV] = KMS_URL_DEV
	KMS_URL_MAP[TEST] = KMS_URL_TEST
	KMS_URL_MAP[PROD] = KMS_URL_PROD

	AUTH_URL_MAP[DEV] = AUTH_URL_DEV
	AUTH_URL_MAP[TEST] = AUTH_URL_TEST
	AUTH_URL_MAP[PROD] = AUTH_URL_PROD
}

func GetInstance(param Params) (*KmsEx, error) {
	err := checkKmsExParams(param)
	if err != nil {
		return nil, err
	}
	if GKmsEx == nil {
		lock.Lock()
		defer lock.Unlock()
		if param.EnvCode == 0 {
			GKmsEx = &KmsEx{
				AppName:     param.AppName,
				Ak:          param.Ak,
				Sk:          param.Sk,
				EnvCode:     param.EnvCode,
				KMSTransObj: GetKMSTransObject(param.AppName, param.Ak, param.Sk, param.KmsUrl, param.AuthUrl),
			}
		} else {
			GKmsEx = &KmsEx{
				AppName:     param.AppName,
				Ak:          param.Ak,
				Sk:          param.Sk,
				EnvCode:     param.EnvCode,
				KMSTransObj: GetKMSTransObject(param.AppName, param.Ak, param.Sk, KMS_URL_MAP[param.EnvCode], AUTH_URL_MAP[param.EnvCode]),
			}
		}
		InitMap()
	} else {
		if GKmsEx.AppName == param.AppName {
			return nil, errors.WithMessage(kmserr.ErrParasInvalid, "appname exists")
		}
		if GKmsEx.Ak == param.Ak {
			return nil, errors.WithMessage(kmserr.ErrParasInvalid, "ak exists")
		}
		if GKmsEx.Sk == param.Sk {
			return nil, errors.WithMessage(kmserr.ErrParasInvalid, "Sk exists")
		}
	}

	return GKmsEx, nil
}

func checkKmsExParams(param Params) error {
	if param.AppName == "" {
		return errors.WithMessage(kmserr.ErrParasInvalid, "App name is null")
	}
	if param.Ak == "" {
		return errors.WithMessage(kmserr.ErrParasInvalid, "ak is null")
	}
	if param.Sk == "" {
		return errors.WithMessage(kmserr.ErrParasInvalid, "sk is null")
	}
	_, err := hex.DecodeString(param.Sk)
	if err != nil {
		return errors.Wrapf(kmserr.ErrHexString, "hex.DecodeString(hexSK): %v", err)
	}

	if param.EnvCode == 0 {
		// check url
	} else {
		if param.EnvCode != DEV && param.EnvCode != TEST && param.EnvCode != PROD {
			return errors.WithMessagef(kmserr.ErrParasInvalid, "envcode(%v) invalid", param.EnvCode)
		}
	}

	return nil
}

func (kmsex *KmsEx) Init() error {
	if IsInit {
		return kmserr.ErrRepeatInit
	}
	err := kmsex.KMSTransObj.Init()
	if err != nil {
		return err
	}

	// register itself
	err = kmsex.AddKmsCipherService()
	if err != nil {
		return err
	}
	// get granted appnames
	targetAppNames, err := kmsex.KMSTransObj.GetGrantedAppNames()
	if err != nil {
		return err
	}

	// register target appnames in bulk
	appNameSize := len(targetAppNames)
	if appNameSize > 0 {
		if appNameSize+kmsex.KMS_CIPHER_MAP.Count() >= MAX_SERVER {
			return kmserr.ErrMaxApps
		}
		// register
		// if appNameSize <= 100 {
		appInfos, err := kmsex.KMSTransObj.GetGrantedAppInfo(targetAppNames)
		if err != nil {
			return err
		}
		appSecInfos := appInfos.AppInfos
		if len(appSecInfos) > 0 {
			kmsex.UpdateKmsCipherServiceMap(appSecInfos)
		}
	}

	// update keypairs
	err = kmsex.UpdateKeyPairs()
	if err != nil {
		return err
	}

	// start task
	kmsex.StartTimerTask()

	IsInit = true
	return nil
}

// AddKmsCipherService ... register server itself
func (kmsex *KmsEx) AddKmsCipherService() error {
	kek, err := kmsex.KMSTransObj.GetKEK(kmsex.AppName)
	if err != nil {
		return errors.WithMessage(err, "init failed, cannot get service kek")
	}

	cipherParams := CipherParams{
		AppName:     kmsex.AppName,
		KmsTransObj: kmsex.KMSTransObj,
		Kek:         kek,
	}
	kmsExCipher, err := GetKmsExCipher(cipherParams)
	if err != nil {
		return err
	}
	err = kmsExCipher.Init()
	if err != nil {
		return err
	}

	// update access time

	// update kmsex cipher object into ciphermap
	kmsex.KMS_CIPHER_MAP.Set(kmsex.AppName, kmsExCipher)

	return nil
}

// UpdateKmsCipherServiceMap ...
func (kmsex *KmsEx) UpdateKmsCipherServiceMap(appInfos []model.AppSecInfo) error {
	// for debug, check current kmscipherservice map info
	// if kmsex.KMS_CIPHER_MAP == nil {
	// 	fmt.Printf("master(%v) kms_cipher_map size: 0\n", kmsex.AppName)
	// } else {
	// 	fmt.Printf("master(%v) kms_cipher_map size: %v\n", kmsex.AppName, kmsex.KMS_CIPHER_MAP.Count())
	// }
	// fmt.Printf("master(%v) kms_cipher_map size: %v\n", kmsex.AppName, kmsex.KMS_CIPHER_MAP.Count())

	for _, appInfo := range appInfos {
		secPolicy := &model.SecurityPolicy{
			DegradeFlag:       appInfo.AllowDegradation,
			TicketDegradeFlag: appInfo.AllowUnauthenticated,
			TicketExpired:     false,
		}
		kekBytes, _ := base64.StdEncoding.DecodeString(appInfo.Kek)
		cipherParams := CipherParams{
			AppName:        appInfo.AppName,
			KmsTransObj:    kmsex.KMSTransObj,
			Kek:            kekBytes,
			SecurityPolicy: secPolicy,
		}

		var tmpKmsExCipher interface{}
		tmpKmsExCipher, _ = kmsex.KMS_CIPHER_MAP.Get(appInfo.AppName)
		if tmpKmsExCipher == nil {
			tmpKmsExCipher, _ = GetKmsExCipher(cipherParams)
		}
		kmsExCipher := tmpKmsExCipher.(*KmsExCipher)

		// kmsExCipher = tmpKmsExCipher.(*KmsExCipher)

		// fmt.Printf("appname: %v ", appInfo.AppName)
		wbMap, _ := kmsex.GetWBMap(appInfo.Wbs)
		kmsExCipher.WBMap = wbMap
		// if kmsExCipher.WBMap == nil {
		// 	fmt.Printf("slave(%v) wbmap size: 0", appInfo.AppName)
		// } else {
		// 	fmt.Printf("slave(%v) wbmap size: %v", appInfo.AppName, kmsExCipher.WBMap.Count())
		// }

		// if kmsExCipher.KeyPairMap == nil {
		// 	fmt.Printf(" appname(%v) keypairmap size: 0\n", appInfo.AppName)
		// } else {
		// 	fmt.Printf(" keypairmap size: %v\n", kmsExCipher.KeyPairMap.Count())
		// }
		kmsex.KMS_CIPHER_MAP.Set(kmsExCipher.AppName, kmsExCipher)
	}

	return nil
}

// UpdateKeyPairs ...
func (kmsex *KmsEx) UpdateKeyPairs() error {
	grantedAppNames, err := kmsex.KMSTransObj.GetGrantedAppNames()
	if err != nil {
		return errors.WithMessagef(kmserr.ErrInternal, "updatekeypairs failed, %v", err)
	}

	appAllNames := make([]string, len(grantedAppNames)+1)
	n := copy(appAllNames, grantedAppNames)
	appAllNames[n] = kmsex.AppName

	resDownloadKeyPair, err := kmsex.KMSTransObj.DownloadKeyPair(appAllNames)
	if err != nil {
		return errors.WithMessagef(kmserr.ErrInternal, "downloadKeyPair failed, %v", err)
	}

	appKeyPairs := resDownloadKeyPair.AppKeyPairInfos

	for _, appKeyPair := range appKeyPairs {
		appName := appKeyPair.AppName
		kmsExCipher, isOk := kmsex.KMS_CIPHER_MAP.Get(appName)
		if !isOk || kmsExCipher == nil {
			continue
		}
		keyPairMap := kmsExCipher.(*KmsExCipher).KeyPairMap
		if keyPairMap == nil {
			keyPairMap = cmap.New()
		}
		keyPairs := appKeyPair.KeyPairs
		for _, keyPair := range keyPairs {
			keyPairMap.Set(keyPair.PKHash, keyPair)
		}

		kmsExCipher.(*KmsExCipher).KeyPairMap = keyPairMap
		// fmt.Printf("appname(%v) keypair size: %v\n", appName, keyPairMap.Count())
	}

	// for _, appKeyPair := range appKeyPairs {
	// 	appName := appKeyPair.AppName
	// 	kmsExCipher, isOk := kmsex.KMS_CIPHER_MAP.Get(appName)
	// 	if !isOk || kmsExCipher == nil {
	// 		continue
	// 	}
	// 	keyPairMap := kmsExCipher.(*KmsExCipher).KeyPairMap
	// 	fmt.Printf("appname(%v) keypair size: %v\n\n", appName, keyPairMap.Count())
	// }
	return nil
}

// GetWBMap
func (kmsex *KmsEx) GetWBMap(wbs []model.WBs) (cmap.ConcurrentMap, error) {
	// handle wbs

	wbMap := cmap.New()
	if len(wbs) == 0 {
		return nil, nil
	}
	for _, wbInfo := range wbs {
		wbId := wbInfo.WbId
		wbKeys := wbInfo.WbKey
		// handle wbKeys

		wbKeyMap := cmap.New()
		for _, wbKeyInfo := range wbKeys {
			keyBytes, _ := base64.StdEncoding.DecodeString(wbKeyInfo.Key)
			wbKeyMap.Set(wbKeyInfo.KeyId, keyBytes)
			wbMap.Set(wbId, wbKeyMap)
			// fmt.Printf("wbid(%s), wbkeyid(%s), wbkey(%s)\n", wbId, wbKeyInfo.KeyId, hex.EncodeToString(keyBytes))
		}
	}

	return wbMap, nil
}

// ParseEnvelopePack ...
func (kmsex *KmsEx) ParseEnvelopePack(packSource []byte) (*KmsExCipher, bool, error) {
	if packSource == nil {
		return nil, false, errors.WithMessage(kmserr.ErrPackDataInvalid, "packSource nil")
	}

	reqEnvelopPack := &model.EnvelopePack{}
	err := json.Unmarshal(packSource, reqEnvelopPack)
	if err != nil {
		return nil, false, errors.WithMessagef(kmserr.ErrPackDataInvalid, "json.Unmarshal(reqBodyBytes, reqEnvelopPack): %v", err)
	}
	if reqEnvelopPack.ParsePayload != "1" {
		return nil, false, errors.WithMessagef(kmserr.ErrPackDataInvalid, "envelop: ParsePayload(%v) invalid, maybe not envelop data", reqEnvelopPack.ParsePayload)
	}

	envelope := reqEnvelopPack.CMS
	cmsVersion := envelope.Version
	if cmsVersion != "1" {
		return nil, true, errors.WithMessagef(kmserr.ErrPackDataInvalid, "envelop: cmsVersion(%v) invalid", envelope.Version)
	}
	workMode := envelope.WorkMode
	if workMode != "mode_envelope_rsa" {
		return nil, true, errors.WithMessagef(kmserr.ErrPackDataInvalid, "envelop: workMode(%v) invalid", workMode)
	}

	appName := envelope.AppName
	if appName == "" {
		return nil, true, errors.WithMessagef(kmserr.ErrPackDataInvalid, "envelop: appname nil(%v)", appName)
	}

	kmsExCipher, isOk := kmsex.KMS_CIPHER_MAP.Get(appName)
	if !isOk {
		return nil, true, errors.WithMessagef(kmserr.ErrInternal, "Not have permission1: appname(%v)", appName)
	}
	if kmsExCipher == nil {
		return nil, true, errors.WithMessagef(kmserr.ErrInternal, "Not have permission2: appname(%v)", appName)
	}

	tmpKmsExCipher := &KmsExCipher{
		AppName:     appName,
		KmsTransObj: kmsex.KMSTransObj,
		Envelope:    &envelope,
		KeyPairMap:  kmsExCipher.(*KmsExCipher).KeyPairMap,
	}

	return tmpKmsExCipher, true, nil
}

// GetKmsExCipher ...
func (kmsex *KmsEx) GetKmsExCipher(packSource []byte) (*KmsExCipher, error) {
	if packSource == nil {
		return nil, kmserr.ErrPackDataInvalid
	}
	if len(packSource) == 0 {
		return nil, kmserr.ErrPackDataInvalid
	}

	// check pack is envelop data stream
	// handle envelop data
	enCipher, isEnvelopPack, err := kmsex.ParseEnvelopePack(packSource)
	if err == nil {
		return enCipher, nil
	}
	if isEnvelopPack && err != nil {
		return nil, err
	}

	// handle cms data
	reqPack := &model.Pack{}
	err = json.Unmarshal(packSource, reqPack)
	if err != nil {
		return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "json.Unmarshal(reqBodyBytes, reqPack): %v", err)
	}

	reqHeader := &model.Header{}
	err = json.Unmarshal([]byte(reqPack.Header), reqHeader)
	if err != nil {
		err = errors.Wrapf(kmserr.ErrPackDataInvalid, "json.Unmarshal(reqPack.Header, a.reqHeader): %v", err)
		return nil, err
	}

	reqPayload := []byte(reqPack.Payload)
	reqSign := []byte(reqPack.Signature)
	if reqSign == nil {
		return nil, errors.WithMessage(kmserr.ErrPackDataInvalid, "signature of pack data is nil")
	}

	cmsSignData := &model.CMSSignedData{}
	err = json.Unmarshal(reqSign, cmsSignData)
	if err != nil {
		return nil, errors.WithMessage(kmserr.ErrPackDataInvalid, "signed data format invalid")
	}
	cmsPayloadData := &model.CMSEncryptedData{}
	err = json.Unmarshal(reqPayload, cmsPayloadData)
	if err != nil {
		return nil, errors.WithMessage(kmserr.ErrPackDataInvalid, "CMSEncryptedData format invalid")
	}

	// check header and signed data's field valid

	keyType := reqHeader.KeyType
	var appName string
	if keyType == KEY_TYPE_WB {
		appName = reqHeader.WbKeyIndex.AppName
	} else if keyType == KEY_TYPE_SESSION_KEY {
		appName = reqHeader.UakIndex.AppName
	} else {
		return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "appname(%v) invalid", appName)
	}
	if appName == "" {
		return nil, errors.WithMessagef(kmserr.ErrPackDataInvalid, "appname(%v) invalid", appName)
	}

	kmsExCipher, isOk := kmsex.KMS_CIPHER_MAP.Get(appName)
	if !isOk {
		return nil, errors.WithMessagef(kmserr.ErrInternal, "Not have permission1: appname(%v)", appName)
	}
	if kmsExCipher == nil {
		return nil, errors.WithMessagef(kmserr.ErrInternal, "Not have permission2: appname(%v)", appName)
	}

	// get plain text of signature
	sigPlain := reqPack.Header + reqPack.Payload
	encryptAlgorithm := cmsPayloadData.Alg
	padding := cmsPayloadData.Padding
	hashId := cmsSignData.HashId
	signAlg := cmsSignData.SignAlg
	kek := kmsExCipher.(*KmsExCipher).Kek
	secPolicy := kmsExCipher.(*KmsExCipher).SecurityPolicy
	tmpKmsExCipher := &KmsExCipher{
		AppName:            appName,
		SignedData:         cmsSignData,
		Header:             reqHeader,
		Payload:            cmsPayloadData,
		KmsTransObj:        kmsex.KMSTransObj,
		Kek:                kek,
		SecurityPolicy:     secPolicy,
		WBMap:              kmsExCipher.(*KmsExCipher).WBMap,
		EncryptAlgorithm:   encryptAlgorithm,
		Padding:            padding,
		SignatureAlgorithm: signAlg,
		HashId:             hashId,
		SignaturePlain:     []byte(sigPlain),
		Pack:               reqPack,
	}

	// get mk and dek from pack
	err = tmpKmsExCipher.GenMkAndDek()
	if err != nil {
		return nil, err
	}

	return tmpKmsExCipher, nil
}

// // TaskUpdateKmsCipherService ...
// func (kmsex *KmsEx) TaskUpdateKmsCipherService() {

// }

// TaskUpdateCipherServerMap ...
func (kmsex *KmsEx) TaskUpdateCipherServerMap() error {
	grantedAppNames, err := kmsex.KMSTransObj.GetGrantedAppNames()
	if err != nil {
		return err
	}
	// delete local old granted appnames
	// and update newest granted appnames
	if grantedAppNames == nil {
		return err
	}
	appAllNames := make([]string, len(grantedAppNames)+1)
	n := copy(appAllNames, grantedAppNames)
	appAllNames[n] = kmsex.AppName

	grantedAppInfos, err := kmsex.KMSTransObj.GetGrantedAppInfo(appAllNames)
	if err != nil {
		return err
	}
	if grantedAppInfos == nil {
		return err
	}
	if grantedAppInfos.AppInfos == nil {
		return err
	}

	err = kmsex.UpdateKmsCipherServiceMap(grantedAppInfos.AppInfos)
	return err
}

// StartTimerTask ...
func (kmsex *KmsEx) StartTimerTask() {
	// start UpdateKeyPairs
	t1 := cron.New()
	min1 := 1

	spec1 := fmt.Sprintf("0 */%d * * * ?", min1)
	t1.AddFunc(spec1, func() {
		err := kmsex.UpdateKeyPairs()
		if err != nil {
			fmt.Printf("UpdateKeyPairs failed, %v\n", err)
		}
	})
	t1.Start()

	// start updateCipherServerMap, update security policy and wbkeys
	t2 := cron.New()
	min2 := 1
	spec2 := fmt.Sprintf("0 */%d * * * ?", min2)
	t2.AddFunc(spec2, func() {
		err := kmsex.TaskUpdateCipherServerMap()
		if err != nil {
			fmt.Printf("TaskUpdateCipherServerMap failed, %v\n", err)
		}
	})
	t2.Start()
}
