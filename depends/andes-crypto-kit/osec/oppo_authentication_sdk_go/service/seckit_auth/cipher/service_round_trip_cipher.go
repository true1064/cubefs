package cipher

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/cw"
	authClient "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/auth_client"
	authClientType "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/auth_client/types"
	manager "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/seckit_auth/manager"
	authType "oppo.com/andes-crypto/kit/osec/oppo_authentication_sdk_go/service/seckit_auth/types"
)

type SecKitAuthServiceRoundTripCipher struct {
	SName             string `json:"s_name"`
	DegradeSwitch     bool
	AuthClient        *authClient.AuthClient
	KekManager        *manager.KekManager
	PermissionManager *manager.PermissionManager
	BlackListManager  *manager.BlackListManager
	SecpolicyManager  *manager.SecpolicyManager
	Unseal            bool
	Ticket            *authType.Ticket
	ReqHeader         *authClientType.Header
	SigAlg            authClientType.SigAlgType
	EncAlg            authClientType.EncAlgType
	CName             string
}

func (s *SecKitAuthServiceRoundTripCipher) Seal(plain []byte) ([]byte, error) {
	if !s.Unseal {
		return nil, errors.New(authType.AUTH_ERR_UNSEAL_FIRST)
	}
	dek := ""
	mk := ""
	securityLevel := int(authType.Low)
	if s.Ticket != nil {
		dek = s.Ticket.EncKey
		mk = s.Ticket.MacKey
		securityLevel = s.Ticket.SecLevel
	}
	pack, err := GetPack(plain, s.ReqHeader, dek, mk, securityLevel)
	packBytes, _ := json.Marshal(pack)
	return packBytes, err
}

func (s *SecKitAuthServiceRoundTripCipher) UnSeal(cipherPack []byte) ([]byte, error) {
	reqPack := &authClientType.Pack{}
	json.Unmarshal(cipherPack, reqPack)
	if reqPack.AuthProtoGuard != AUTH_PROTO_GUARD {
		return nil, errors.New(authType.AUTH_ERR_INVALID_PROTOCOL_DATA)
	}
	s.DegradeSwitch = s.SecpolicyManager.GetDetgadeSwitch()
	headerStr := reqPack.Header
	reqHeader := &authClientType.Header{}
	unErr := json.Unmarshal([]byte(headerStr), reqHeader)
	if unErr != nil {
		return nil, unErr
	}
	s.ReqHeader = reqHeader
	err := s.parseHeader()
	if err != nil {
		return nil, err
	}
	if s.Ticket != nil {
		dek := s.Ticket.EncKey
		mk := s.Ticket.MacKey
		if s.Ticket.SecLevel == int(authType.High) || s.Ticket.SecLevel == int(authType.Middle) {
			err := VerifyPack(reqPack, mk, authClientType.SigAlgType(s.SigAlg))
			if err != nil {
				return nil, err
			}
		}
		if s.Ticket.SecLevel == int(authType.High) {
			plain, err := Decrypt([]byte(reqPack.Body), dek, authClientType.EncAlgType(s.EncAlg))
			s.Unseal = true
			return plain, err
		} else {
			s.Unseal = true
			return []byte(reqPack.Body), nil
		}
	} else {
		s.AuthClient.HealthCheck()
		s.Unseal = true
		return []byte(reqPack.Body), nil
	}
}

func (s *SecKitAuthServiceRoundTripCipher) parseHeader() error {
	if !strings.EqualFold(s.ReqHeader.SName, s.SName) {
		return errors.New("invalid service name in header")
	}
	s.EncAlg = s.ReqHeader.EncAlg
	s.SigAlg = s.ReqHeader.SigAlg
	s.CName = s.ReqHeader.CName
	wrapTicket := &authType.WrapTicket{}
	tickerBytes, _ := base64.StdEncoding.DecodeString(s.ReqHeader.Ticket)
	json.Unmarshal(tickerBytes, wrapTicket)
	if wrapTicket != nil {
		kek, kekErr := s.KekManager.GetKek(wrapTicket.KeyVersion)
		if kekErr != nil || kek == nil {
			return kekErr
		}
		kekBytes, _ := base64.StdEncoding.DecodeString(kek.Kek)
		ticketSource, err := cw.AESGCMPackDecrypt(kekBytes[:16], []byte(wrapTicket.Ticket))
		if err != nil {
			return err
		}
		ticket := &authType.Ticket{}
		unErr := json.Unmarshal(ticketSource, ticket)
		if unErr != nil {
			return unErr
		}
		s.Ticket = ticket
		if s.Ticket.EndTime < time.Now().Unix() {
			return errors.New("ticket expired")
		}
		if !strings.EqualFold(s.ReqHeader.CName, s.Ticket.CName) {
			return errors.New("invalid client name in header")
		}
		if !strings.EqualFold(s.ReqHeader.SName, s.Ticket.SName) {
			return errors.New("invalid service name in header")
		}
		authenTicatorBytes, err := Decrypt([]byte(s.ReqHeader.AuthenTicator), s.Ticket.EncKey, s.EncAlg)
		authenTicator := &authType.Authenticator{}
		json.Unmarshal(authenTicatorBytes, authenTicator)
		if !strings.EqualFold(authenTicator.EntityInfo.HostName, s.Ticket.EntityInfo.HostName) || !strings.EqualFold(authenTicator.CName, s.Ticket.CName) {
			return errors.New("invalid belonging of ticket")
		}
		if s.BlackListManager.IsHostNameInBlackList(s.Ticket.EntityInfo.HostName) {
			return errors.New("client in black list")
		}
	}

	return nil
}

func (s *SecKitAuthServiceRoundTripCipher) IsResourceAccessPermitted(reqAction, reqResource string) (bool, error) {
	if !s.Unseal {
		return false, errors.New(authType.AUTH_ERR_UNSEAL_FIRST)
	}
	if s.Ticket.PermissionInfo.PermissionLocation == authType.LocationLocal {
		if s.Ticket.PermissionInfo.PermissionLevel == int(authClientType.APPLICATION) {
			return true, nil
		}
		for _, v := range s.Ticket.PermissionInfo.Items {
			if strings.EqualFold(v.ResourceAction, reqAction) {
				if reqResource == "" {
					return true, nil
				}
				for _, source := range v.Resources {
					if strings.EqualFold(source, reqResource) {
						return true, nil
					}
				}
			}
		}
	} else {
		return s.PermissionManager.IsResourceAccessPermitted(s.Ticket.CName, reqAction, reqResource), nil
	}
	return false, nil
}
