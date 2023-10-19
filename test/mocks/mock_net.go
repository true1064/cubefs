package mocks

import (
	"io"
	"net"
	"time"
)

type MockNet struct {
	wOff int
	rOff int
	data []byte
}

func (m *MockNet) Read(b []byte) (n int, err error) {
	if m.rOff >= m.wOff {
		return 0, io.EOF
	}

	//rSize := len(b)
	//if m.wOff > len(b)+m.rOff {
	//	rSize = m.wOff - m.rOff
	//}
	rSize := copy(b, m.data[m.rOff:])
	m.rOff += rSize
	return rSize, nil
}

// Write writes data to the connection.
// Write can be made to time out and return an error after a fixed
// time limit; see SetDeadline and SetWriteDeadline.
func (m *MockNet) Write(b []byte) (n int, err error) {
	m.data = append(m.data, b...)
	n = len(b)
	m.wOff += n
	return n, nil
}

func (m *MockNet) Close() error                     { return nil }
func (m *MockNet) LocalAddr() net.Addr              { return nil }
func (m *MockNet) RemoteAddr() net.Addr             { return nil }
func (m *MockNet) SetDeadline(time.Time) error      { return nil }
func (m *MockNet) SetReadDeadline(time.Time) error  { return nil }
func (m *MockNet) SetWriteDeadline(time.Time) error { return nil }
