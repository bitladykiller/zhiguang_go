package counter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/redislock"
	"go.uber.org/zap"
)

const (
	defaultCounterFailureTaskBatchSize = 100
	defaultCounterFailureTaskInterval  = time.Minute
	defaultCounterFailureCleanupBatch  = 500
	defaultCounterFailureCleanupPeriod = time.Hour
	defaultCounterFailureDoneRetention = 7 * 24 * time.Hour
	maxCounterFailureTaskRetryDelay    = 5 * time.Minute
)

var errCounterFailureTaskTerminal = errors.New("counter failure task terminal")

// CounterFailureWorker 负责消费 MySQL 中的失败任务，并定时清理已完成记录。
//
// 任务分两类：
//   - publish：补发 Kafka 事件
//   - apply/flush：从 bitmap 重新计算指定 metric 的绝对值并覆写对应 SDS 槽位
type CounterFailureWorker struct {
	store            CounterFailureTaskStore
	service          *CounterService
	logger           *zap.Logger
	batchSize        int
	interval         time.Duration
	cleanupBatchSize int
	cleanupInterval  time.Duration
	doneRetention    time.Duration
	nextCleanupAt    time.Time
}

func NewCounterFailureWorker(
	store CounterFailureTaskStore,
	service *CounterService,
	logger *zap.Logger,
	cfg *config.CounterConfig,
) *CounterFailureWorker {
	if store == nil || service == nil || service.redis == nil {
		return nil
	}
	if cfg != nil && !cfg.Repair.Enabled {
		return nil
	}

	batchSize := defaultCounterFailureTaskBatchSize
	interval := defaultCounterFailureTaskInterval
	cleanupBatchSize := defaultCounterFailureCleanupBatch
	cleanupInterval := defaultCounterFailureCleanupPeriod
	doneRetention := defaultCounterFailureDoneRetention
	if cfg != nil {
		if cfg.Repair.BatchSize > 0 {
			batchSize = cfg.Repair.BatchSize
		}
		if cfg.Repair.IntervalMs > 0 {
			interval = time.Duration(cfg.Repair.IntervalMs) * time.Millisecond
		}
		if cfg.Repair.CleanupBatchSize > 0 {
			cleanupBatchSize = cfg.Repair.CleanupBatchSize
		}
		if cfg.Repair.CleanupIntervalMs > 0 {
			cleanupInterval = time.Duration(cfg.Repair.CleanupIntervalMs) * time.Millisecond
		}
		if cfg.Repair.DoneRetentionHours > 0 {
			doneRetention = time.Duration(cfg.Repair.DoneRetentionHours) * time.Hour
		}
	}

	return &CounterFailureWorker{
		store:            store,
		service:          service,
		logger:           logger,
		batchSize:        batchSize,
		interval:         interval,
		cleanupBatchSize: cleanupBatchSize,
		cleanupInterval:  cleanupInterval,
		doneRetention:    doneRetention,
	}
}

func (w *CounterFailureWorker) Start(ctx context.Context) {
	if w == nil {
		return
	}

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		w.processOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *CounterFailureWorker) processOnce(ctx context.Context) {
	if w == nil || w.store == nil {
		return
	}

	tasks, err := w.store.ClaimPending(ctx, w.batchSize)
	if err != nil {
		w.logWarn("claim counter failure tasks failed", err)
		return
	}

	for _, task := range tasks {
		if task == nil {
			continue
		}
		if err := w.handleTask(ctx, task); err != nil {
			if errors.Is(err, errCounterFailureTaskTerminal) {
				if markErr := w.store.MarkDone(ctx, task.ID); markErr != nil {
					w.logWarn("mark terminal counter failure task done failed", markErr)
				}
				continue
			}

			retryCount := task.RetryCount + 1
			nextRetryAt := time.Now().Add(counterFailureRetryDelay(retryCount))
			if markErr := w.store.MarkRetry(ctx, task.ID, retryCount, nextRetryAt, failureErrorMessage(err)); markErr != nil {
				w.logWarn("mark counter failure task retry failed", markErr)
			}
			continue
		}

		if err := w.store.MarkDone(ctx, task.ID); err != nil {
			w.logWarn("mark counter failure task done failed", err)
		}
	}

	w.cleanupDoneTasks(ctx)
}

func (w *CounterFailureWorker) handleTask(ctx context.Context, task *CounterFailedMessage) error {
	if task == nil {
		return terminalCounterFailureTaskError("nil counter failure task")
	}

	switch task.Stage {
	case counterFailureStagePublish:
		return w.republishEvent(ctx, task)
	case counterFailureStageApply, counterFailureStageFlush:
		return w.repairMetric(ctx, task)
	default:
		return terminalCounterFailureTaskError("unknown counter failure stage: " + task.Stage)
	}
}

func (w *CounterFailureWorker) republishEvent(ctx context.Context, task *CounterFailedMessage) error {
	if w.service == nil || w.service.producer == nil {
		return fmt.Errorf("counter producer is nil")
	}

	var event CounterEvent
	if err := json.Unmarshal([]byte(task.Payload), &event); err != nil {
		return terminalCounterFailureTaskError("invalid publish task payload")
	}
	if event.EntityType == "" || event.EntityID == "" || event.Metric == "" || event.Delta == 0 {
		return terminalCounterFailureTaskError("publish task payload missing required counter fields")
	}

	return w.service.producer.Publish(&event)
}

func (w *CounterFailureWorker) repairMetric(ctx context.Context, task *CounterFailedMessage) error {
	if task.EntityType == "" || task.EntityID == "" || task.Metric == "" {
		return terminalCounterFailureTaskError("apply task missing entity or metric")
	}

	lockKey := fmt.Sprintf("lock:sds-repair:%s:%s:%s", task.EntityType, task.EntityID, task.Metric)
	lock, locked, err := redislock.TryAcquire(ctx, w.service.redis, lockKey, w.service.rebuildLockOptions)
	if err != nil {
		return err
	}
	if !locked {
		return fmt.Errorf("counter repair lock not acquired")
	}
	defer lock.Release()

	if err := w.service.repairMetricFromBitmap(ctx, task.EntityType, task.EntityID, task.Metric); err != nil {
		return err
	}
	w.service.resetBackoff(ctx, task.EntityType, task.EntityID)
	return nil
}

func (w *CounterFailureWorker) logWarn(msg string, err error) {
	if w != nil && w.logger != nil {
		w.logger.Warn(msg, zap.Error(err))
	}
}

func (w *CounterFailureWorker) cleanupDoneTasks(ctx context.Context) {
	if w == nil || w.store == nil {
		return
	}
	if w.cleanupBatchSize <= 0 || w.cleanupInterval <= 0 || w.doneRetention <= 0 {
		return
	}

	now := time.Now()
	if !w.nextCleanupAt.IsZero() && now.Before(w.nextCleanupAt) {
		return
	}

	before := now.Add(-w.doneRetention)
	if _, err := w.store.DeleteDoneBefore(ctx, before, w.cleanupBatchSize); err != nil {
		w.logWarn("cleanup counter failure done tasks failed", err)
	}
	w.nextCleanupAt = now.Add(w.cleanupInterval)
}

func counterFailureRetryDelay(retryCount int) time.Duration {
	if retryCount <= 0 {
		return time.Second
	}

	delay := time.Second << (retryCount - 1)
	if delay > maxCounterFailureTaskRetryDelay {
		return maxCounterFailureTaskRetryDelay
	}
	return delay
}

func terminalCounterFailureTaskError(message string) error {
	return fmt.Errorf("%w: %s", errCounterFailureTaskTerminal, message)
}
