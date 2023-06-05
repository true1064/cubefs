package auth_client

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/robfig/cron"

	"andescryptokit/osec/oppo_authentication_sdk_go/cw"
	authClientType "andescryptokit/osec/oppo_authentication_sdk_go/service/auth_client/types"
	authType "andescryptokit/osec/oppo_authentication_sdk_go/service/seckit_auth/types"
	util "andescryptokit/osec/oppo_authentication_sdk_go/util"
)

type AuthClient struct {
	AppName           string                  `json:"appName"`
	AuthUrl           string                  `json:"authUrl"`
	AK                string                  `json:"ak"`
	SK                string                  `json:"sk"`
	AppEntityInfo     *authType.AppEntityInfo `json:"appEntityInfo"`
	SessionKeyForAKSK *authType.SessionKey
	TicketKeeper      *authType.TicketKeeper
	mux               sync.RWMutex
}

const OPPO_AUTH = "appauth"

func (s *AuthClient) Init() error {
	key, err := util.GenerateKey(s.AK, s.SK)
	if err != nil {
		return errors.New(authType.AUTH_ERR_AUTH_INVALID_PARAM_FORMAT)
	}
	s.SessionKeyForAKSK = key
	s.HealthCheck()
	ticketKeeper, err := s.updateAuthTicket()
	if err != nil {
		return err
	}
	s.mux.Lock()
	s.TicketKeeper = ticketKeeper
	s.mux.Unlock()

	c := cron.New()
	spec := "0 */55 * * * ?"
	c.AddFunc(spec, func() {
		fmt.Println("cron running update authTicket")
		ticketKeeper, err := s.updateAuthTicket()
		if err != nil {
			fmt.Println(err)
		}
		s.mux.Lock()
		s.TicketKeeper = ticketKeeper
		s.mux.Unlock()
	})
	c.Start()
	return nil
}

func (s *AuthClient) copyAuthTicket() *authType.TicketKeeper {
	var keeper *authType.TicketKeeper
	s.mux.RLock()
	if s.TicketKeeper != nil {
		keeper = s.TicketKeeper
	}
	s.mux.RUnlock()
	timeNow := time.Now().Unix()
	if keeper == nil || s.TicketKeeper.SessionKey.EndTime-60 < timeNow {
		s.mux.Lock()
		s.TicketKeeper, _ = s.updateAuthTicket()
		keeper = s.TicketKeeper
		s.mux.Unlock()
	}
	return keeper
}

func (s *AuthClient) HealthCheck() {
	resp, err := http.Get(s.AuthUrl + "/biz")
	if err != nil || resp.StatusCode != 200 {
		fmt.Println(errors.New(authClientType.AUTH_ERR_HEALTH_CHECK_FAILED))
	}
}

func (s *AuthClient) updateAuthTicket() (*authType.TicketKeeper, error) {
	ticketKeeper := &authType.TicketKeeper{}
	httpClient := util.GetHttpClient(s.AuthUrl+"/as/"+authClientType.AS_API_GetAuthTicket, map[string]string{"Content-Type": "application/octet-stream"})
	req := &authClientType.GetAuthTicketReqParam{}
	getAuthTicketReqBody := &authClientType.GetAuthTicketReqBody{
		EntityInfo: s.AppEntityInfo,
		CName:      s.AppName,
		SName:      OPPO_AUTH,
	}
	requestID, _ := util.GetUuid()
	nonce, _ := util.GetUuid()
	getAuthTicketReqHeader := &authClientType.GetAuthTicketReqHeader{
		Version:        int32(1),
		RequestID:      requestID,
		CredentialId:   s.AK,
		EncAlg:         authClientType.NO_ENC_ALG,
		SigAlg:         authClientType.HS256,
		Timestamp:      uint64(time.Now().Unix()),
		Nonce:          nonce,
		CredentialType: authClientType.AKSK,
	}
	headerByte, _ := json.Marshal(getAuthTicketReqHeader)
	bodyByte, _ := json.Marshal(getAuthTicketReqBody)
	signPlain := bytes.Join([][]byte{headerByte, bodyByte}, nil)
	ek, _ := base64.StdEncoding.DecodeString(s.SessionKeyForAKSK.EK)
	mk, _ := base64.StdEncoding.DecodeString(s.SessionKeyForAKSK.MK)
	req.Header = string(headerByte)
	req.Body = string(bodyByte)
	req.Signature = base64.StdEncoding.EncodeToString(util.SignWithHmca(signPlain, mk, util.SHA_256))
	reqBytes, _ := json.Marshal(req)
	respBytes, err := httpClient.Post(reqBytes, nil)
	if err != nil {
		return nil, err
	}
	resp := &authClientType.GetAuthTicketRespParam{}
	json.Unmarshal(respBytes, resp)
	respBody := &authClientType.GetAuthTicketRespBody{}
	json.Unmarshal([]byte(resp.Body), respBody)
	sessionKeyBytes, _ := cw.AESGCMPackDecrypt(ek[:16], []byte(respBody.SessionKey))
	ticketKeeper.SessionKey = &authType.SessionKey{}
	json.Unmarshal(sessionKeyBytes, ticketKeeper.SessionKey)
	ticketKeeper.Ticket = respBody.Ticket
	return ticketKeeper, nil
}

func (s *AuthClient) getPack(param []byte, ticketKeeper *authType.TicketKeeper) []byte {
	ek, _ := base64.StdEncoding.DecodeString(ticketKeeper.SessionKey.EK)
	mk, _ := base64.StdEncoding.DecodeString(ticketKeeper.SessionKey.MK)
	authenticator := &authType.Authenticator{
		CName:      s.AppName,
		EntityInfo: s.AppEntityInfo,
	}
	authenticatorBytes, _ := json.Marshal(authenticator)
	authenticatorCipher, _ := cw.AESGCMPackEncrypt(ek[:16], authenticatorBytes)
	req := &authClientType.Pack{}
	requestID, _ := util.GetUuid()
	nonce, _ := util.GetUuid()
	header := &authClientType.Header{
		CName:          s.AppName,
		SName:          OPPO_AUTH,
		Version:        int32(1),
		RequestID:      requestID,
		CredentialId:   s.AK,
		EncAlg:         authClientType.AES_128_GCM_NOPADDING,
		SigAlg:         authClientType.HS256,
		Timestamp:      uint64(time.Now().Unix()),
		Nonce:          nonce,
		CredentialType: authClientType.TICKET,
		Ticket:         ticketKeeper.Ticket,
		AuthenTicator:  string(authenticatorCipher),
	}
	eBody, _ := cw.AESGCMPackEncrypt(ek[:16], param)
	headerBytes, _ := json.Marshal(header)
	req.Header = string(headerBytes)
	req.Body = string(eBody)
	signPlain := bytes.Join([][]byte{headerBytes, eBody}, nil)
	req.Signature = base64.StdEncoding.EncodeToString(util.SignWithHmca(signPlain, mk, util.SHA_256))
	reqBytes, _ := json.Marshal(req)
	return reqBytes
}

func (s *AuthClient) verifyPack(packet []byte, sessionKey *authType.SessionKey) (msg []byte, err error) {
	resp := &authClientType.Pack{}
	json.Unmarshal(packet, resp)
	ek, _ := base64.StdEncoding.DecodeString(sessionKey.EK)
	mk, _ := base64.StdEncoding.DecodeString(sessionKey.MK)
	respSig, _ := base64.StdEncoding.DecodeString(resp.Signature)
	headerBytes := []byte(resp.Header)
	bodyBytes := []byte(resp.Body)
	signPlain := bytes.Join([][]byte{headerBytes, bodyBytes}, nil)
	if !util.VerifyWithHmac(signPlain, respSig, mk, util.SHA_256) {
		return nil, errors.New("signature illegal")
	}
	msg, err = cw.AESGCMPackDecrypt(ek[:16], bodyBytes)
	return msg, err
}

// Client获取访问service的Ticket
func (s *AuthClient) GetPeerTicket(peerAppName string) (*authType.TicketKeeper, error) {
	ticketKeeper := s.copyAuthTicket()
	req := &authClientType.GetPeerTicketReqParam{
		PeerName: peerAppName,
	}
	reqBytes, _ := json.Marshal(req)
	packByte := s.getPack(reqBytes, ticketKeeper)
	// packByte := s.getPack(reqBytes, ticketKeeper)
	httpClient := util.GetHttpClient(s.AuthUrl+"/biz/"+authClientType.GET_PEER_TICKET, map[string]string{"Content-Type": "application/octet-stream"})
	resp, err := httpClient.Post(packByte, nil)
	if err != nil {
		return nil, err
	}
	// msg, err := s.verifyPack(resp, s.TicketKeeper.SessionKey)
	msg, err := s.verifyPack(resp, ticketKeeper.SessionKey)
	respBody := &authClientType.GetPeerTicketRespParam{}
	json.Unmarshal(msg, respBody)

	peeerTicketKeeper := &authType.TicketKeeper{}
	peeerTicketKeeper.SessionKey = respBody.SessionKey
	peeerTicketKeeper.Ticket = respBody.Ticket
	return peeerTicketKeeper, err
}

// 获取Service安全策略
func (s *AuthClient) GetSecPolicy() (*authClientType.GetSecPolicyRespParam, error) {
	ticketKeeper := s.copyAuthTicket()
	respBody := &authClientType.GetSecPolicyRespParam{}
	packByte := s.getPack([]byte("{}"), ticketKeeper)
	httpClient := util.GetHttpClient(s.AuthUrl+"/biz/"+authClientType.GET_SEC_POLICY, map[string]string{"Content-Type": "application/octet-stream"})
	resp, err := httpClient.Post(packByte, nil)
	if err != nil {
		return nil, err
	}
	msg, err := s.verifyPack(resp, ticketKeeper.SessionKey)
	json.Unmarshal(msg, respBody)
	return respBody, err
}

func (s *AuthClient) GetKeks() (*authClientType.GetKeksRespParam, error) {
	ticketKeeper := s.copyAuthTicket()
	respBody := &authClientType.GetKeksRespParam{}
	packByte := s.getPack([]byte("{}"), ticketKeeper)
	httpClient := util.GetHttpClient(s.AuthUrl+"/biz/"+authClientType.GET_KEKS, map[string]string{"Content-Type": "application/octet-stream"})
	resp, err := httpClient.Post(packByte, nil)
	if err != nil {
		return nil, err
	}
	msg, err := s.verifyPack(resp, ticketKeeper.SessionKey)
	json.Unmarshal(msg, respBody)
	return respBody, err
}

// Service获取所有授权信息
func (s *AuthClient) GetPermissions(req *authClientType.GetPermissionsReqParam) (*authClientType.GetPermissionsRespParam, error) {
	ticketKeeper := s.copyAuthTicket()
	respBody := &authClientType.GetPermissionsRespParam{}
	reqBytes, _ := json.Marshal(req)
	packByte := s.getPack(reqBytes, ticketKeeper)
	httpClient := util.GetHttpClient(s.AuthUrl+"/biz/"+authClientType.GET_PERMISSIONS, map[string]string{"Content-Type": "application/octet-stream"})
	resp, err := httpClient.Post(packByte, nil)
	if err != nil {
		return nil, err
	}
	msg, err := s.verifyPack(resp, ticketKeeper.SessionKey)
	json.Unmarshal(msg, respBody)
	return respBody, err
}

// Service获取所有授权信息
func (s *AuthClient) GetPermittedClients() (*authClientType.GetPermittedClientsRespParam, error) {
	ticketKeeper := s.copyAuthTicket()
	respBody := &authClientType.GetPermittedClientsRespParam{}
	packByte := s.getPack([]byte("{}"), ticketKeeper)
	httpClient := util.GetHttpClient(s.AuthUrl+"/biz/"+authClientType.GET_PERMITTED_CLIENTS, map[string]string{"Content-Type": "application/octet-stream"})
	resp, err := httpClient.Post(packByte, nil)
	if err != nil {
		return nil, err
	}
	msg, err := s.verifyPack(resp, ticketKeeper.SessionKey)
	json.Unmarshal(msg, respBody)
	return respBody, err
}

// Service获取被禁用的Client
func (s *AuthClient) GetBlackList() (*authClientType.GetDisabledClientsRespParam, error) {
	ticketKeeper := s.copyAuthTicket()
	respBody := &authClientType.GetDisabledClientsRespParam{}
	packByte := s.getPack([]byte("{}"), ticketKeeper)
	httpClient := util.GetHttpClient(s.AuthUrl+"/biz/"+authClientType.GET_DISABLED_CLIENTS, map[string]string{"Content-Type": "application/octet-stream"})
	resp, err := httpClient.Post(packByte, nil)
	if err != nil {
		return nil, err
	}
	msg, err := s.verifyPack(resp, ticketKeeper.SessionKey)
	json.Unmarshal(msg, respBody)
	return respBody, err
}

// service 验证token合法性
func (s *AuthClient) VerifyToken(req *authClientType.VerifyTokenReqParam) (*authClientType.VerifyTokenRespParam, error) {
	ticketKeeper := s.copyAuthTicket()
	respBody := &authClientType.VerifyTokenRespParam{}
	reqBytes, _ := json.Marshal(req)
	packByte := s.getPack(reqBytes, ticketKeeper)
	httpClient := util.GetHttpClient(s.AuthUrl+"/biz/"+authClientType.VERIFY_TOKEN, map[string]string{"Content-Type": "application/octet-stream"})
	resp, err := httpClient.Post(packByte, nil)
	if err != nil {
		return nil, err
	}
	msg, err := s.verifyPack(resp, ticketKeeper.SessionKey)
	json.Unmarshal(msg, respBody)
	return respBody, err
}

// service 获取用户的认证凭据
func (s *AuthClient) GetUserCredential(req *authClientType.GetUserCredentialReqParam) (*authClientType.GetUserCredentialRespParam, error) {
	ticketKeeper := s.copyAuthTicket()
	respBody := &authClientType.GetUserCredentialRespParam{}
	reqBytes, _ := json.Marshal(req)
	packByte := s.getPack(reqBytes, ticketKeeper)
	httpClient := util.GetHttpClient(s.AuthUrl+"/biz/"+authClientType.GET_USER_CREDENTIAL, map[string]string{"Content-Type": "application/octet-stream"})
	resp, err := httpClient.Post(packByte, nil)
	if err != nil {
		return nil, err
	}
	msg, err := s.verifyPack(resp, ticketKeeper.SessionKey)
	json.Unmarshal(msg, respBody)
	return respBody, err
}

// service 获取用户基本信息
func (s *AuthClient) GetUsersInfo(req *authClientType.GetUsersInfoReqParam) (*authClientType.GetUsersInfoRespParam, error) {
	ticketKeeper := s.copyAuthTicket()
	respBody := &authClientType.GetUsersInfoRespParam{}
	reqBytes, _ := json.Marshal(req)
	packByte := s.getPack(reqBytes, ticketKeeper)
	httpClient := util.GetHttpClient(s.AuthUrl+"/biz/"+authClientType.GET_USERS_INFO, map[string]string{"Content-Type": "application/octet-stream"})
	resp, err := httpClient.Post(packByte, nil)
	if err != nil {
		return nil, err
	}
	msg, err := s.verifyPack(resp, ticketKeeper.SessionKey)
	json.Unmarshal(msg, respBody)
	return respBody, err
}

func (s *AuthClient) GetAllUserBrief() (*authClientType.GetAllUserBriefRespParam, error) {
	ticketKeeper := s.copyAuthTicket()
	respBody := &authClientType.GetAllUserBriefRespParam{}
	packByte := s.getPack([]byte("{}"), ticketKeeper)
	httpClient := util.GetHttpClient(s.AuthUrl+"/biz/"+authClientType.GET_ALL_USER_BRIEF, map[string]string{"Content-Type": "application/octet-stream"})
	resp, err := httpClient.Post(packByte, nil)
	if err != nil {
		return nil, err
	}
	msg, err := s.verifyPack(resp, ticketKeeper.SessionKey)
	json.Unmarshal(msg, respBody)
	return respBody, err
}
