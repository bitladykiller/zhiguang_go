package outbox

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/contextutil"
)

// Cleaner 周期性删除 outbox 表中超过保留期的已投递事件。
//
// WHY 必须存在：
//
//	标准部署（canal.enabled=true）下，outbox 表的消费者是 **binlog**：
//	Canal 读的是变更日志，而不是表本身，因此行被插入后**永远不会被读取**，
//	更不会被删除（DELETE 只存在于备用的 DirectPoll/PollConsumer 路径，标准部署不启用）。
//	每次发帖、编辑、关注、取关各插一行——表只增不减，是一枚运维定时炸弹。
//
//	删行对链路完全安全：Canal 消费的是 INSERT 的 binlog 事件，行的后续删除
//	（其 DELETE binlog 会被消费端按变更类型过滤掉）不影响任何已投递或在途的消息；
//	保留期的意义只是给人工排障留一段可回看的窗口。
//
// 分批删除（LIMIT batch 循环）避免一条大事务长时间持锁。
type Cleaner struct {
	db        *sqlx.DB
	logger    *zap.Logger
	retention time.Duration
	interval  time.Duration
	batch     int
}

// CleanerConfig 控制清理节奏。零值字段取默认：保留 7 天、每小时一轮、每批 1000 行。
type CleanerConfig struct {
	Retention time.Duration
	Interval  time.Duration
	Batch     int
}

// NewCleaner 创建 outbox 清理任务；db 为 nil 时返回 nil（bootstrap 跳过注册）。
func NewCleaner(db *sqlx.DB, cfg CleanerConfig, logger *zap.Logger) *Cleaner {
	if db == nil {
		return nil
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	if cfg.Retention <= 0 {
		cfg.Retention = 7 * 24 * time.Hour
	}
	if cfg.Interval <= 0 {
		cfg.Interval = time.Hour
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 1000
	}
	return &Cleaner{db: db, logger: logger, retention: cfg.Retention, interval: cfg.Interval, batch: cfg.Batch}
}

// Start 阻塞运行清理循环，直到 ctx 取消（满足 BackgroundRunner 契约）。
func (c *Cleaner) Start(ctx context.Context) {
	if c == nil {
		return
	}
	// 启动即清一轮：长期停机重启后不必再等一个完整 interval。
	c.cleanOnce(ctx)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.cleanOnce(ctx)
		}
	}
}

// String 供监督器展示可读名称。
func (c *Cleaner) String() string { return "outbox-cleaner" }

// cleanOnce 分批删除保留期外的行，直到没有更多或 ctx 取消。
func (c *Cleaner) cleanOnce(ctx context.Context) {
	cutoff := time.Now().Add(-c.retention)
	var total int64
	for {
		res, err := c.db.ExecContext(ctx,
			`DELETE FROM outbox WHERE created_at < ? LIMIT ?`, cutoff, c.batch)
		if err != nil {
			if ctx.Err() == nil {
				c.logger.Warn("outbox cleaner delete failed", zap.Error(err))
			}
			return
		}
		n, _ := res.RowsAffected()
		total += n
		if n < int64(c.batch) {
			break
		}
		// 批间小憩，把写压力摊平（也响应停机信号）。
		if !contextutil.Sleep(ctx, 50*time.Millisecond) {
			return
		}
	}
	if total > 0 {
		c.logger.Info("outbox cleaner removed expired events",
			zap.Int64("rows", total), zap.Time("cutoff", cutoff))
	}
}
