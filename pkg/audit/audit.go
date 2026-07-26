// Package audit 提供审计日志的结构化记录能力（谁、何时、对什么、做了什么）。
package audit

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/contextutil"
)

type Action string

const (
	ActionLogin         Action = "login"
	ActionLogout        Action = "logout"
	ActionRegister      Action = "register"
	ActionDeletePost    Action = "delete_post"
	ActionUpdatePost    Action = "update_post"
	ActionCreatePost    Action = "create_post"
	ActionDeleteComment Action = "delete_comment"
	ActionFollow        Action = "follow"
	ActionUnfollow      Action = "unfollow"
	ActionLike          Action = "like"
	ActionUnlike        Action = "unlike"
)

type AuditLogger struct {
	logger *zap.Logger
}

func NewAuditLogger(logger *zap.Logger) *AuditLogger {
	return &AuditLogger{logger: logger.With(zap.String("component", "audit"))}
}

func (a *AuditLogger) LogAction(ctx context.Context, action Action, userID int64, resourceType, resourceID, detail string) {
	fields := []zap.Field{
		zap.String("action", string(action)),
		zap.Int64("user_id", userID),
		zap.String("resource_type", resourceType),
		zap.String("resource_id", resourceID),
		zap.String("detail", detail),
		zap.Time("timestamp", time.Now()),
	}

	if traceID := contextutil.TraceIDFrom(ctx); traceID != "" {
		fields = append(fields, zap.String("trace_id", traceID))
	}

	a.logger.Info("audit", fields...)
}
