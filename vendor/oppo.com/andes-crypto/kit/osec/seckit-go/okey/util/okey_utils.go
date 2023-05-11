package util

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	logs "oppo.com/andes-crypto/kit/osec/seckit-go/log"
)

// StringSliceEqual ...
func StringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	if (a == nil) != (b == nil) {
		return false
	}

	for i, v := range a {
		if v != b[i] {
			return false
		}
	}

	return true
}

// GetExternalIP ...
func GetExternalIP() (net.IP, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			ip := getIPFromAddr(addr)
			if ip == nil {
				continue
			}
			return ip, nil
		}
	}
	return nil, errors.New("connected to the network?")
}

// getIPFromAddr ...
func getIPFromAddr(addr net.Addr) net.IP {
	var ip net.IP
	switch v := addr.(type) {
	case *net.IPNet:
		ip = v.IP
	case *net.IPAddr:
		ip = v.IP
	}
	if ip == nil || ip.IsLoopback() {
		return nil
	}
	ip = ip.To4()
	if ip == nil {
		return nil // not an ipv4 address
	}

	return ip
}

// GetCurFuncName ...
func GetCurFuncName() string {
	pc := make([]uintptr, 1)
	runtime.Callers(2, pc)
	f := runtime.FuncForPC(pc[0])
	return f.Name()
}

// TimeCost ...
func TimeCost() func(funcName string, reqID *string) {
	start := time.Now()
	return func(funcName string, reqID *string) {
		var tmpReqId string = ""
		if reqID != nil {
			tmpReqId = *reqID
		}
		logs.LogDebugf(tmpReqId, "%v cost %v ms ==============>>", funcName, time.Since(start).Milliseconds())
	}
}

// GetCurrentPath ... get process running path
func GetCurrentPath() (string, error) {
	file, err := exec.LookPath(os.Args[0])
	if err != nil {
		return "", err
	}
	path, err := filepath.Abs(file)
	if err != nil {
		return "", err
	}
	i := strings.LastIndex(path, "/")
	if i < 0 {
		i = strings.LastIndex(path, "\\")
	}

	return string(path[0 : i+1]), nil
}

// CheckExpired check if input time range expired
// before: start time of input time range, 0 means ignore start time
// after: end time of input time range, 0 means ignore end time
func CheckExpired(before uint64, after uint64) error {
	now := uint64(time.Now().UTC().Unix())
	if before != 0 && now < before {
		return errors.New("expired")
	}

	if after != 0 && now > after {
		return errors.New("expired")
	}

	return nil
}

// IsDigitStr ...
func IsDigitStr(str string) bool {
	_, err := strconv.Atoi(str)
	if err != nil {
		return false
	}

	return true
}

// ReadFileContents ...
func ReadFileContents(filepath string) (string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		fmt.Printf("open file failed: err: %v\n", err)
		return "", errors.New("open wb file failed")
	}
	defer file.Close()

	fileBytes, _ := ioutil.ReadAll(file)

	return string(fileBytes), nil
}

func GetCurFuncNameWithoutPath() string {
	pc := make([]uintptr, 1)
	runtime.Callers(2, pc)
	f := runtime.FuncForPC(pc[0])
	parts := strings.Split(f.Name(), ".")
	pl := len(parts)
	return parts[pl-1]
}
