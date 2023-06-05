/*
Copyright OPPO Corp. All Rights Reserved.
*/

package errno

// 定义错误码
type Errno struct {

	// code 错误码。
	code int

	// message 详细出错信息。
	message string
}

// Error 返回出错信息。
//
//	@receiver e
//	@return string 出错信息。
func (e Errno) Error() string {
	return e.message
}

// Message 获取出错信息。
//
//	@receiver e
//	@return string 出错信息。
func (e Errno) Message() string {
	return e.message
}

// Reload 重新加载出错信息。
//
//	@receiver e
//	@param message 出错信息。
//	@return Errno 错误指针。
func (e Errno) Reload(message string) *Errno {
	e.message = message
	return &e
}

// Append 在已有的错误信息上追加一些额外的补充信息。
//
//	@receiver e
//	@param message 待补充的错误信息。
//	@return *Errno 错误指针。
func (e Errno) Append(message string) *Errno {
	e.message += message
	return &e
}

// Code 获取出错错误码。
//
//	@receiver e
//	@return int 错误码。
func (e Errno) Code() int {
	return e.code
}
