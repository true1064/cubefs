package log

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"

	logs "github.com/astaxie/beego/logs"
)

var logLevelMap = map[string]int{
	"critical": logs.LevelCritical,
	"error":    logs.LevelError,
	"info":     logs.LevelInfo,
	"notice":   logs.LevelNotice,
	"debug":    logs.LevelDebug,
	"all":      logs.LevelDebug,
}

// Logger
var (
	AuditLogger *logs.BeeLogger
	DebugLogger *logs.BeeLogger
)

// LogInit initialize beego logs
func LogInit(logPath string, logLevel string) {
	// logs.Async()
	logs.EnableFuncCallDepth(true)
	logs.SetLogFuncCallDepth(5)
	if len(logPath) > 0 {
		arg := `{"filename":"` + logPath + `"}`
		logs.SetLogger(logs.AdapterFile, arg)
	} else {
		logs.SetLogger(logs.AdapterConsole, `{"console":"stdout"}`)
	}

	// level
	level, ok := logLevelMap[strings.ToLower(logLevel)]
	if ok != true {
		LogErrorfWithoutReqID("LogLevel set failed.input logLevel(%s).", logLevel)
	}

	logs.SetLevel(level)
}

// KmsLogInit is used init kms logger
//@auditlogPath  is the output path of audit log
//@auditlogLevel is audit log level
//@debugPath is the output path of debug log
//@debugLogLevel is debug log level
func KmsLogInit(auditlogPath string, auditlogLevel string, debugPath string, debugLogLevel string) {
	AuditLogger = logs.NewLogger()
	DebugLogger = logs.NewLogger()
	// logs.Async()
	AuditLogger.EnableFuncCallDepth(true)
	DebugLogger.EnableFuncCallDepth(true)

	if len(auditlogPath) > 0 {
		arg := `{"filename":"` + auditlogPath + `"}`
		AuditLogger.SetLogger(logs.AdapterFile, arg)
	} else {
		AuditLogger.SetLogger(logs.AdapterConsole, `{"console":"stdout"}`)
	}

	if len(debugPath) > 0 {
		arg := `{"filename":"` + debugPath + `"}`
		DebugLogger.SetLogger(logs.AdapterFile, arg)
	} else {
		DebugLogger.SetLogger(logs.AdapterConsole, `{"console":"stdout"}`)
	}

	AuditLogger.SetLogFuncCallDepth(4)
	DebugLogger.SetLogFuncCallDepth(4)

	// level
	debugLevel, ok := logLevelMap[strings.ToLower(debugLogLevel)]
	if ok != true {
		LogErrorfWithoutReqID("LogLevel set failed.input logLevel(%s).", debugLogLevel)
	}
	auditLevel, ok := logLevelMap[strings.ToLower(auditlogLevel)]
	if ok != true {
		LogErrorfWithoutReqID("LogLevel set failed.input logLevel(%s).", auditlogLevel)
	}
	DebugLogger.SetLevel(debugLevel)
	AuditLogger.SetLevel(auditLevel)
}

// LogCriticalf log critical
func LogCriticalf(requestID string, format string, a ...interface{}) {
	prefix := "[ReqID:" + requestID + "] "
	logs.Critical(prefix+format, a...)
}

// KmsLogCriticalf log critical

func KmsAuditLogCriticalf(kal *KmsAuditLog) {
	var kalByte []byte
	if kal != nil {
		kalByte, _ = json.Marshal(kal)
		AuditLogger.Critical(string(kalByte))
	}
}

func KmsDebugLogCriticalf(kdl *KmsDebugLog) {
	var kdlByte []byte
	if kdl != nil {
		kdlByte, _ = json.Marshal(kdl)
		DebugLogger.Critical(string(kdlByte))
	}
}

func KmsAllLogCriticalf(kal *KmsAuditLog, kdl *KmsDebugLog) {
	KmsAuditLogCriticalf(kal)
	KmsDebugLogCriticalf(kdl)
}

// LogErrorf log error
func LogErrorf(requestID string, format string, a ...interface{}) {
	prefix := "[ReqID:" + requestID + "] "
	logs.Error(prefix+format, a...)
}

func KmsAuditLogErrorf(kal *KmsAuditLog) {
	var kalByte []byte
	if kal != nil {
		kalByte, _ = json.Marshal(kal)
		AuditLogger.Error(string(kalByte))
	}
}

func KmsDebugLogErrorf(kdl *KmsDebugLog) {
	var kdlByte []byte
	if kdl != nil {
		kdlByte, _ = json.Marshal(kdl)
		DebugLogger.Error(string(kdlByte))
	}
}

func KmsAllLogLogErrorf(kal *KmsAuditLog, kdl *KmsDebugLog) {
	KmsAuditLogErrorf(kal)
	KmsDebugLogErrorf(kdl)
}

// LogNoticef log info
func LogNoticef(requestID string, format string, a ...interface{}) {
	prefix := "[ReqID:" + requestID + "] "
	logs.Notice(prefix+format, a...)
}

func KmsAuditLogNoticef(kal *KmsAuditLog) {
	var kalByte []byte
	if kal != nil {
		kalByte, _ = json.Marshal(kal)
		AuditLogger.Notice(string(kalByte))
	}
}

func KmsDebugLogNoticef(kdl *KmsDebugLog) {
	var kdlByte []byte
	if kdl != nil {
		kdlByte, _ = json.Marshal(kdl)
		DebugLogger.Notice(string(kdlByte))
	}
}

func KmsAllLogLogNoticef(kal *KmsAuditLog, kdl *KmsDebugLog) {
	KmsAuditLogNoticef(kal)
	KmsDebugLogNoticef(kdl)
}

// LogInfof log debug
func LogInfof(requestID string, format string, a ...interface{}) {
	prefix := "[ReqID:" + requestID + "] "
	logs.Info(prefix+format, a...)
}

func KmsAllLogInfof(kal *KmsAuditLog, kdl *KmsDebugLog) {
	KmsAuditLogInfof(kal)
	KmsDebugLogInfof(kdl)
}

func KmsDebugLogInfof(kdl *KmsDebugLog) {
	if kdl != nil {
		buffer := &bytes.Buffer{}
		encoder := json.NewEncoder(buffer)
		encoder.SetEscapeHTML(false)
		encoder.Encode(kdl)
		kdlByte := buffer.Bytes()
		DebugLogger.Info(string(kdlByte))
	}
}

func KmsAuditLogInfof(kal *KmsAuditLog) {
	var kdlByte []byte
	if kal != nil {
		kdlByte, _ = json.Marshal(*kal)
		AuditLogger.Info(string(kdlByte))
	}
}

// LogDebugf log debug
func LogDebugf(requestID string, format string, a ...interface{}) {
	prefix := "[ReqID:" + requestID + "] "
	logs.Debug(prefix+format, a...)
}

func KmsAuditLogDebugf(kal *KmsAuditLog) {
	var kalByte []byte
	if kal != nil {
		kalByte, _ = json.Marshal(kal)
		AuditLogger.Debug(string(kalByte))
	}
}

func KmsDebugLogDebugf(kdl *KmsDebugLog) {
	var kdlByte []byte
	if kdl != nil {
		kdlByte, _ = json.Marshal(kdl)
		AuditLogger.Debug(string(kdlByte))
	}
}

func KmsAllLogLogDebugf(kal *KmsAuditLog, kdl *KmsDebugLog) {
	KmsAuditLogDebugf(kal)
	KmsDebugLogDebugf(kdl)
}

// LogCriticalfWithoutReqID log critical with out requestID
func LogCriticalfWithoutReqID(format string, a ...interface{}) {
	logs.Critical(format, a...)
}

// LogErrorfWithoutReqID log error without requestID
func LogErrorfWithoutReqID(format string, a ...interface{}) {
	logs.Error(format, a...)
}

// LogNoticefWithoutReqID log info without requestID
func LogNoticefWithoutReqID(format string, a ...interface{}) {
	logs.Notice(format, a...)
}

// LogInfofWithoutReqID log debug without requestID
func LogInfofWithoutReqID(format string, a ...interface{}) {
	logs.Info(format, a...)
}

// LogDebugfWithoutReqID log debug without requestID
func LogDebugfWithoutReqID(format string, a ...interface{}) {
	logs.Debug(format, a...)
}

// KmsAuditLog is audit log information
type KmsAuditLog struct {
	Time         time.Time
	RequestID    string
	UserCategory string
	UserID       string
	APIName      string
	// APICost 		int64
	APIVersion string
	ErrCode    string
}

// KmsDebugLog is debug log information
type KmsDebugLog struct {
	Time         time.Time
	RequestID    string
	UserCategory string
	UserID       string
	MethodsName  string
	Stack        string
	ErrCode      string
	Msg          string
}
