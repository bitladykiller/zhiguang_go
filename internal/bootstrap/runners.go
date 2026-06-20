package bootstrap

import (
	"context"

	"github.com/zhiguang/app/internal/cache"
)

// hotKeyRunner 将 cache.HotKeyDetector 适配为 server.BackgroundRunner，
// 使其后台 flush goroutine 能随服务生命周期启动和退出。
type hotKeyRunner struct {
	d *cache.HotKeyDetector
}

func (r *hotKeyRunner) Start(ctx context.Context) {
	r.d.Run(ctx)
}
