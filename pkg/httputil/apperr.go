package httputil

import (
	"errors"

	"github.com/zhiguang/app/pkg/errcode"
)

// ToAppError 将任意 error 转为 *errcode.AppError，供 handler 统一走 response.Error。
func ToAppError(err error) *errcode.AppError {
	var appErr *errcode.AppError
	if errors.As(err, &appErr) {
		return appErr
	}
	return errcode.ErrInternal.WithMsg(err.Error())
}