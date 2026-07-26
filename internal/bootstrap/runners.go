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

// Start 阻塞运行热点探测的 flush 循环，直到 ctx 取消。
//
// 这里必须调用 RunUntilDone 而非 Run：BackgroundRunner 的契约是
// 「Start 阻塞至任务生命周期结束」，上层 server.App 依赖这一点判断后台任务是否排空。
// 早期实现调用的是非阻塞的 Run，Start 立刻返回导致 WaitGroup 提前归零，
// 停机时最后一个窗口的热点计数会被丢弃。
func (r *hotKeyRunner) Start(ctx context.Context) {
	r.d.RunUntilDone(ctx)
}
