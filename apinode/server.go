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

package apinode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"golang.org/x/time/rate"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/cubefs/cubefs/apinode/crypto"
	"github.com/cubefs/cubefs/apinode/drive"
	"github.com/cubefs/cubefs/blobstore/common/profile"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/rpc/auditlog"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/stat"
)

func init() {
	trace.RequestIDKey = "x-cfa-trace-id"
	trace.PrefixBaggage = "x-cfa-baggage-"
	trace.FieldKeyTraceID = "x-cfa-trace-id"
	trace.FieldKeySpanID = "x-cfa-span-id"
}

// Configuration items that act on the api node.
const (
	configListen     = proto.ListenPort
	configMasterAddr = proto.MasterAddr
	configLogDir     = "logDir"
	configLogLevel   = "logLevel"

	headerService = "x-cfa-service"
)

// Default of configuration value
const (
	defaultListen = ":80"
	serviceDrive  = "drive"
	servicePosix  = "posix"
	serviceHdfs   = "hdfs"
	serviceS3     = "s3"
	limiterBrust  = 2000
)

type rpcNode interface {
	RegisterAPIRouters() *rpc.Router
	Start(cfg *config.Config) error
	closer.Closer
}

type muxNode interface {
	RegisterAPIRouters() *mux.Route
	Start(cfg *config.Config) error
	closer.Closer
}

type apiNode struct {
	listen     string
	httpServer *http.Server
	mc         *master.MasterClient
	control    common.Control

	audit  auditlog.Config
	defers []func() // close logger after http server closed
	router struct {
		Drive struct {
			node    rpcNode
			handler http.Handler
		}
		S3 struct {
			node    muxNode
			handler http.Handler
		}
		Posix struct {
			node    rpcNode
			handler http.Handler
		}
		Hdfs struct {
			node    rpcNode
			handler http.Handler
		}
	}
}

func (s *apiNode) Start(cfg *config.Config) error {
	return s.control.Start(s, cfg, handleStart)
}

func (s *apiNode) Shutdown() {
	s.control.Shutdown(s, handleShutdown)
}

func (s *apiNode) Sync() {
	s.control.Sync()
}

func (s *apiNode) loadConfig(cfg *config.Config) error {
	logDir := cfg.GetString(configLogDir)
	if logDir != "" {
		log.SetOutput(&lumberjack.Logger{
			Filename:   path.Join(logDir, "apinode.log"),
			MaxSize:    1024,
			MaxAge:     7,
			MaxBackups: 7,
			LocalTime:  true,
		})

		s.audit.LogDir = path.Join(logDir, "audit")
		s.audit.LogFileSuffix = ".log"
		s.audit.Backup = 30
		s.audit.MetricConfig = auditlog.PrometheusConfig{
			EnableHttpMethod:    true,
			EnableReqLengthCnt:  true,
			EnableRespLengthCnt: true,
			EnableRespDuration:  true,
			EnableXWarnCnt:      true,
			MaxApiLevel:         3,
		}
	}

	strLevel := cfg.GetString(configLogLevel)
	var logLevel log.Level
	if err := logLevel.UnmarshalYAML(func(l interface{}) error {
		if x, ok := l.(*string); ok {
			*x = strLevel
		}
		return nil
	}); err != nil {
		logLevel = log.Lwarn
	}
	log.SetOutputLevel(logLevel)

	listen := cfg.GetString(configListen)
	if len(listen) == 0 {
		listen = defaultListen
	}
	if _, err := strconv.Atoi(listen); err == nil {
		listen = ":" + listen
	}
	s.listen = listen

	masters := cfg.GetString(configMasterAddr)
	if len(masters) == 0 {
		return config.NewIllegalConfigError(configMasterAddr)
	}
	s.mc = master.NewMasterClientFromString(masters, false)
	stat.NewStatistic(logDir, "stat", int64(stat.DefaultStatLogSize), stat.DefaultTimeOutUs, true)
	return nil
}

func getCryptoConfig(cfg *config.Config) (crypto.Configure, error) {
	var conf crypto.Configure
	c := cfg.GetValue("crypto")
	if c == nil {
		return conf, nil
	}
	data, err := json.Marshal(c)
	if err != nil {
		return conf, err
	}

	if err = json.Unmarshal(data, &conf); err != nil {
		return conf, err
	}
	return conf, nil
}

func (s *apiNode) startRouters(cfg *config.Config) error {
	conf, err := getCryptoConfig(cfg)
	if err != nil {
		return err
	}
	if err := crypto.Init(conf); err != nil {
		return err
	}

	{
		limiter := rate.NewLimiter(rate.Inf, limiterBrust)
		node := drive.New(limiter)
		lh, logf, err := auditlog.Open("drive", &s.audit)
		if err != nil {
			return err
		}
		if logf != nil {
			s.defers = append(s.defers, func() { logf.Close() })
		}

		hs := []rpc.ProgressHandler{newLimiter(limiter)}

		disableAuth := cfg.GetBoolWithDefault("disableAuth", false)

		if !disableAuth {
			if cfg.GetBoolWithDefault("use_test_auth", false) {
				hs = append(hs, newTestAuth())
			} else {
				authServiceAddr := cfg.GetString("auth_service_addr")
				if authServiceAddr == "" {
					return fmt.Errorf("not found auth_service_addr in config file")
				}
				appkey := cfg.GetString("appkey")
				if appkey == "" {
					return fmt.Errorf("not found appkey in config file")
				}
				hs = append(hs, newAuthenticator(authServiceAddr, appkey))
			}
		}
		hs = append(hs, newCryptor(), lh)

		// register only once
		if profileHandler := profile.NewProfileHandler(s.listen); profileHandler != nil {
			hs = append(hs, profileHandler)
		}

		r := node.RegisterAPIRouters()
		s.router.Drive.node = node
		s.router.Drive.handler = rpc.MiddlewareHandlerWith(r, hs...)

		if err := node.Start(cfg); err != nil {
			return err
		}
	}
	// TODO: new posix, hdfs, s3
	return nil
}

func (s *apiNode) handler(resp http.ResponseWriter, req *http.Request) {
	service := req.Header.Get(headerService)
	switch service {
	case serviceDrive:
		s.router.Drive.handler.ServeHTTP(resp, req)
	case servicePosix, serviceS3, serviceHdfs: // TODO
		resp.WriteHeader(http.StatusMethodNotAllowed)
	case "":
		if isLocalRequest(req) {
			s.router.Drive.handler.ServeHTTP(resp, req)
			return
		}
		resp.WriteHeader(http.StatusMethodNotAllowed)
	default:
		resp.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *apiNode) startHTTPServer() (err error) {
	server := &http.Server{
		Addr:    s.listen,
		Handler: http.HandlerFunc(s.handler),
	}

	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Fatal("startHTTPServer: start http server error", err)
			return
		}
	}()

	s.httpServer = server
	return
}

func handleStart(svr common.Server, cfg *config.Config) error {
	s, ok := svr.(*apiNode)
	if !ok {
		return errors.New("invalid node type, not apinode")
	}

	if err := s.loadConfig(cfg); err != nil {
		log.Errorf("load config error: %v", err)
		return err
	}

	// get cluster from master.
	ci, err := s.mc.AdminAPI().GetClusterInfo()
	if err != nil {
		log.Errorf("get cluster info error: %v", err)
		return err
	}

	if err := s.startRouters(cfg); err != nil {
		log.Errorf("start router error: %v", err)
		return err
	}

	if err := s.startHTTPServer(); err != nil {
		log.Errorf("start http server error: %v", err)
		return err
	}
	registerLogLevel()

	role := cfg.GetString("role")
	exporter.Init(role, cfg)
	exporter.RegistConsul(ci.Cluster, role, cfg)

	log.Info("api node started")
	return nil
}

func handleShutdown(svr common.Server) {
	s, ok := svr.(*apiNode)
	if !ok {
		return
	}
	s.shutdown()
}

func (s *apiNode) shutdown() {
	if s.httpServer != nil {
		_ = s.httpServer.Shutdown(context.Background())
		s.httpServer = nil
	}

	defer func() {
		for _, f := range s.defers {
			f()
		}
	}()

	s.router.Drive.node.Close()
	// TODO: close posix, hdfs, s3
}

// NewServer returns empty server.
func NewServer() common.Server {
	return &apiNode{}
}

func registerLogLevel() {
	logLevelPath, logLevelHandler := log.ChangeDefaultLevelHandler()
	profile.HandleFunc(http.MethodPost, logLevelPath, func(c *rpc.Context) {
		logLevelHandler.ServeHTTP(c.Writer, c.Request)
	})
	profile.HandleFunc(http.MethodGet, logLevelPath, func(c *rpc.Context) {
		logLevelHandler.ServeHTTP(c.Writer, c.Request)
	})
}

func isLocalRequest(req *http.Request) bool {
	addr := req.RemoteAddr
	path := req.URL.Path
	if (strings.HasPrefix(addr, "localhost") || strings.HasPrefix(addr, "127.0.0.1")) &&
		(path == "/metrics" || path == "/log/level" || strings.HasPrefix(path, "/debug/")) {
		return true
	}
	return false
}
