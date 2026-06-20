// Package contextutil 提供 context 相关的通用工具函数。
package contextutil

import (
	"context"
	"time"
)

// Sleep 在 ctx 可取消的前提下等待 d 时长。
//
// 与 time.Sleep 不同，此函数在 ctx 被取消时立即返回 false，
// 避免服务关闭时因 sleep 未到期而延长 Shutdown 时间。
//
// 返回值：
//   - true: 正常等待到超时
//   - false: ctx 被取消
func Sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
