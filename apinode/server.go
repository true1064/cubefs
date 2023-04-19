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
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/cubefs/cubefs/apinode/drive"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter"
)

// Configuration items that act on the api node.
const (
	configListen     = proto.ListenPort
	configMasterAddr = proto.MasterAddr
	configLogDir     = "logDir"
	configLogLevel   = "logLevel"
	configService    = "service"
)

// Default of configuration value
const (
	defaultListen = ":80"
	serviceDrive  = "drive"
	servicePosix  = "posix"
	serviceHdfs   = "hdfs"
	serviceS3     = "s3"
)

type serviceNode interface {
	RegisterAPIRouters() *rpc.Router
	Start(cfg *config.Config) error
	closer.Closer
}

type apiNode struct {
	listen      string
	audit       auditlog.Config
	service     string
	serviceNode serviceNode
	mc          *master.MasterClient
	httpServer  *http.Server
	control     common.Control
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
	service := cfg.GetString(configService)
	switch service {
	case serviceDrive:
		s.serviceNode = drive.New()
	case servicePosix, serviceHdfs, serviceS3:
	default:
		return fmt.Errorf("invalid sevice type: %s", service)
	}
	s.service = service

	logDir := cfg.GetString(configLogDir)
	// no app log and audit log if logDir is empty
	if logDir != "" {
		log.SetOutput(&lumberjack.Logger{
			Filename:   path.Join(logDir, service, ".log"),
			MaxSize:    1024,
			MaxAge:     7,
			MaxBackups: 7,
			LocalTime:  true,
		})

		s.audit.LogDir = logDir
		s.audit.LogFileSuffix = ".log"
		s.audit.Backup = 30
	}

	strLevel := cfg.GetString(configLogLevel)
	var logLevel log.Level
	switch strLevel {
	case "debug":
		logLevel = log.Ldebug
	case "info":
		logLevel = log.Linfo
	case "error":
		logLevel = log.Lerror
	default:
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

	return s.serviceNode.Start(cfg)
}

func handleStart(svr common.Server, cfg *config.Config) error {
	s, ok := svr.(*apiNode)
	if !ok {
		return errors.New("invalid node type, not apinode")
	}

	if err := s.loadConfig(cfg); err != nil {
		return err
	}

	// get cluster from master.
	ci, err := s.mc.AdminAPI().GetClusterInfo()
	if err != nil {
		return err
	}

	if err := s.startRestAPI(); err != nil {
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
	s.shutdownRestAPI()
}

func (s *apiNode) startRestAPI() (err error) {
	lh, logf, err := auditlog.Open(s.service, &s.audit)
	if err != nil {
		return err
	}
	if logf != nil {
		defer logf.Close()
	}

	hs := []rpc.ProgressHandler{lh}
	if profileHandler := profile.NewProfileHandler(s.listen); profileHandler != nil {
		hs = append(hs, profileHandler)
	}

	driveNode := &drive.DriveNode{}
	router := driveNode.RegisterAPIRouters()
	handler := rpc.MiddlewareHandlerWith(router, hs...)
	server := &http.Server{
		Addr:    s.listen,
		Handler: handler,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Fatal("startRestAPI: start http server error", err)
			return
		}
	}()
	s.httpServer = server
	return
}

func (s *apiNode) shutdownRestAPI() {
	if s.httpServer != nil {
		_ = s.httpServer.Shutdown(context.Background())
		s.httpServer = nil
	}
	s.serviceNode.Close()
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
