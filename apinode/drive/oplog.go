package drive

import (
	"time"

	"github.com/cubefs/cubefs/apinode/oplog"
)

type OpCode int

const (
	OpUploadFile OpCode = iota + 1
	OpDeleteFile
	OpUpdateFile
	OpCopyFile
	OpRename
	OpCreateDir
	OpDeleteDir
)

func makeOpLog(op OpCode, uid UserID, path string, fields ...interface{}) oplog.Event {
	event := oplog.Event{
		Key:       string(uid),
		Timestamp: time.Now(),
		Fields:    make(map[string]interface{}),
	}
	event.Fields["op"] = int(op)
	event.Fields["path"] = path
	event.Fields["uid"] = string(uid)
	event.Fields["timestamp"] = event.Timestamp.UnixMilli()

	n := len(fields) / 2 * 2
	for i := 0; i < n; i += 2 {
		if key, ok := fields[i].(string); ok {
			event.Fields[key] = fields[i+1]
		}
	}
	return event
}
