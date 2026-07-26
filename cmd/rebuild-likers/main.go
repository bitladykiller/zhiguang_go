// rebuild-likers 从点赞位图离线重建时间序索引（likers ZSet）。
//
// 背景：时间序 ZSet 随 toggle Lua 只对新互动生效；上线前的历史点赞只在位图里，
// 列表只能走回退路径（按 userID 排序、liked_at=0）。本工具按实体全量扫位图，
// 把历史成员以 score=0（时间不可考）ZADD NX 补进索引——补齐后列表回到时间序主路径。
//
// 用法：
//
//	go run ./cmd/rebuild-likers -config config/config-local.yaml \
//	    -entity-type knowpost -entity-id 123 [-metric like|favorite]
//
// 幂等可重跑；NX 语义保证绝不覆盖真实时间戳。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/counter"
	"github.com/zhiguang/app/internal/database"
	"github.com/zhiguang/app/pkg/config"
)

func main() {
	var (
		configPath = flag.String("config", "config/config.yaml", "配置文件路径")
		entityType = flag.String("entity-type", "knowpost", "实体类型")
		entityID   = flag.Uint64("entity-id", 0, "实体 ID（必填）")
		metric     = flag.String("metric", "like", "指标：like 或 favorite")
	)
	flag.Parse()

	if *entityID == 0 {
		fmt.Fprintln(os.Stderr, "用法: rebuild-likers -config <path> -entity-type knowpost -entity-id <id> [-metric like|favorite]")
		os.Exit(2)
	}

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintln(os.Stderr, "init logger:", err)
		os.Exit(1)
	}
	defer func() { _ = logger.Sync() }()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logger.Fatal("load config", zap.Error(err))
	}
	cfg.ApplyDefaults()

	rdb, err := database.NewRedisClient(&cfg.Redis, logger)
	if err != nil {
		logger.Fatal("connect redis", zap.Error(err))
	}
	defer func() { _ = rdb.Close() }()

	svc := counter.NewCounterService(rdb, nil, &cfg.Counter, nil, "", nil, logger, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	added, err := svc.RebuildLikersTimeIndex(ctx, *entityType, *entityID, *metric)
	if err != nil {
		logger.Fatal("rebuild likers time index failed",
			zap.String("entityType", *entityType), zap.Uint64("entityID", *entityID),
			zap.Int64("addedBeforeFailure", added), zap.Error(err))
	}
	logger.Info("rebuild likers time index done",
		zap.String("entityType", *entityType), zap.Uint64("entityID", *entityID),
		zap.String("metric", *metric), zap.Int64("added", added))
}
