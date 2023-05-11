package cipher

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	authClientType "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/auth_client/types"
	manager "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/seckit_auth/manager"
	authType "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/seckit_auth/types"
	util "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/util"
)

type SecKitAuthClientRoundTripCipher struct {
	CName                 string `json:"c_name"`
	SName                 string `json:"s_name"`
	AppEntityInfo         *authType.AppEntityInfo
	TicketToServerManager *manager.TicketManager
	TickerKeeper          *authType.TicketKeeper
}

func (c *SecKitAuthClientRoundTripCipher) Seal(plain []byte, encAlg authClientType.EncAlgType, sigAlg authClientType.SigAlgType) ([]byte, error) {
	if !c.TicketToServerManager.HavePermission {
		return nil, errors.New(authClientType.AUTH_ERR_NO_PERMISSION)
	}
	c.TickerKeeper = c.TicketToServerManager.GetPeerTicket()
	if c.TickerKeeper != nil {
		ticket := c.TickerKeeper.Ticket
		header := c.getHeader(ticket, encAlg, sigAlg)
		dekStr := c.TickerKeeper.SessionKey.EK
		mkStr := c.TickerKeeper.SessionKey.MK
		secLevel := c.TickerKeeper.SessionKey.SecLevel
		pack, err := GetPack(plain, header, dekStr, mkStr, secLevel)
		packBytes, _ := json.Marshal(pack)
		return packBytes, err
	} else {
		header := c.getHeader("", authClientType.NO_ENC_ALG, authClientType.NO_SIG_ALG)
		pack, err := GetPack(plain, header, "", "", int(authType.Low))
		packBytes, _ := json.Marshal(pack)
		return packBytes, err
	}
}

func (c *SecKitAuthClientRoundTripCipher) UnSeal(cipherPack []byte) ([]byte, error) {
	respPack := &authClientType.Pack{}
	json.Unmarshal(cipherPack, respPack)
	if respPack.AuthProtoGuard != AUTH_PROTO_GUARD {
		return nil, errors.New(authType.AUTH_ERR_INVALID_PROTOCOL_DATA)
	}
	headerStr := respPack.Header
	header := &authClientType.Header{}
	json.Unmarshal([]byte(headerStr), header)

	if !strings.EqualFold(header.SName, c.SName) {
		return nil, errors.New("invalid service name in response header")
	}

	if !strings.EqualFold(header.CName, c.CName) {
		return nil, errors.New("invalid client name in response header")
	}

	if c.TickerKeeper != nil {
		encALg := header.EncAlg
		sigAlg := header.SigAlg

		dekStr := c.TickerKeeper.SessionKey.EK
		mkStr := c.TickerKeeper.SessionKey.MK
		secLevel := c.TickerKeeper.SessionKey.SecLevel
		if secLevel == int(authType.High) || secLevel == int(authType.Middle) {
			if err := VerifyPack(respPack, mkStr, sigAlg); err != nil {
				return nil, err
			}
		}
		if secLevel == int(authType.High) {
			return Decrypt([]byte(respPack.Body), dekStr, encALg)
		} else {
			return []byte(respPack.Body), nil
		}

	}
	return []byte(respPack.Body), nil
}

func (c *SecKitAuthClientRoundTripCipher) getHeader(ticket string, encAlg authClientType.EncAlgType, sigAlg authClientType.SigAlgType) *authClientType.Header {
	requestID, _ := util.GetUuid()
	nonce, _ := util.GetUuid()
	header := &authClientType.Header{
		CName:          c.CName,
		SName:          c.SName,
		Version:        int32(1),
		RequestID:      requestID,
		EncAlg:         encAlg,
		SigAlg:         sigAlg,
		Timestamp:      uint64(time.Now().Unix()),
		Nonce:          nonce,
		CredentialType: authClientType.TICKET,
	}
	if ticket != "" {
		authenticator := &authType.Authenticator{
			EntityInfo: c.AppEntityInfo,
			CName:      c.CName,
		}
		authenticatorBytes, _ := json.Marshal(authenticator)
		authenticatorCipher, _ := Encrypt(authenticatorBytes, c.TickerKeeper.SessionKey.EK, authClientType.EncAlgType(encAlg))
		header.Ticket = ticket
		header.AuthenTicator = string(authenticatorCipher)
	}
	return header
}
