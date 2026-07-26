package llm

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/httputil"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

type LlmHandler struct {
	descSvc DescriptionServiceInterface
	ragSvc  RagQueryServiceInterface
	logger  *zap.Logger
}

func NewLlmHandler(descSvc DescriptionServiceInterface, ragSvc RagQueryServiceInterface, logger *zap.Logger) *LlmHandler {
	if logger == nil {
		logger = zap.NewNop() // 测试常以 nil 构造；goroutine 内的日志路径必须 nil 安全
	}
	return &LlmHandler{descSvc: descSvc, ragSvc: ragSvc, logger: logger}
}

func (h *LlmHandler) RegisterRoutes(r *gin.RouterGroup) {
	llm := r.Group("/knowposts")
	{
		llm.POST("/:id/description/suggest", h.SuggestDescription)
		llm.POST("/:id/rag/query", h.RagQuery)
	}
}

func (h *LlmHandler) SuggestDescription(c *gin.Context) {
	_, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	if h.descSvc == nil {
		response.Error(c, errcode.ErrServiceUnavailable.WithMsg("llm description service is unavailable"))
		return
	}

	var req struct {
		Title   string `json:"title" binding:"required"`
		Content string `json:"content" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errcode.ErrBadRequest.WithMsg("invalid request"))
		return
	}

	desc, err := h.descSvc.SuggestDescription(c.Request.Context(), req.Title, req.Content)
	if err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
		return
	}

	response.Success(c, gin.H{"description": desc})
}

func (h *LlmHandler) RagQuery(c *gin.Context) {
	_, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	if h.ragSvc == nil {
		response.Error(c, errcode.ErrServiceUnavailable.WithMsg("rag query service is unavailable"))
		return
	}

	postID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Error(c, errcode.ErrBadRequest.WithMsg("invalid post id"))
		return
	}

	var req struct {
		Question string `json:"question" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errcode.ErrBadRequest.WithMsg("invalid request"))
		return
	}

	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.WriteHeader(200)

	ctx := c.Request.Context()
	streamChan := make(chan string, 10)

	done := h.startRagStream(ctx, postID, req.Question, streamChan)
	h.pumpSSE(c, ctx, streamChan, done)
}

// startRagStream 在后台 goroutine 中执行 RAG 查询，token 经 streamChan 送出。
// 返回 done：goroutine 结束（正常返回或 panic 恢复）时关闭。
// 使用请求上下文穿透 goroutine，客户端断开时自动取消 RAG 查询。
func (h *LlmHandler) startRagStream(ctx context.Context, postID uint64, question string, streamChan chan string) <-chan struct{} {
	done := make(chan struct{})
	ragCtx, ragCancel := context.WithCancel(ctx)
	go func() {
		defer ragCancel()
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				h.logger.Error("ragSvc.Query panicked", zap.Any("panic", r))
				select {
				case streamChan <- "data: {\"error\": \"internal server error\"}\n\n":
				default:
				}
				select {
				case streamChan <- "data: [DONE]\n\n":
				default:
				}
			}
		}()
		if err := h.ragSvc.Query(ragCtx, postID, question, streamChan); err != nil {
			// 错误已通过 streamChan 以 SSE error 事件送达客户端；此处仅落日志。
			h.logger.Warn("rag query failed", zap.Uint64("postID", postID), zap.Error(err))
		}
	}()
	return done
}

// pumpSSE 把 streamChan 中的 token 边收边刷给客户端，整体限时 30s；
// 退出前持续排空 streamChan 直到生产者 goroutine 结束，避免其在发送时永久阻塞（goroutine 泄漏）。
func (h *LlmHandler) pumpSSE(c *gin.Context, ctx context.Context, streamChan chan string, done <-chan struct{}) {
	flusher, _ := c.Writer.(interface{ Flush() })

	readCtx, readCancel := context.WithTimeout(ctx, 30*time.Second)
	defer readCancel()
	for {
		select {
		case <-readCtx.Done():
			goto cleanup
		case token, ok := <-streamChan:
			if !ok {
				goto cleanup
			}
			fmt.Fprint(c.Writer, token)
			if flusher != nil {
				flusher.Flush()
			}
		}
	}

cleanup:
	for {
		select {
		case <-done:
			return
		case <-streamChan:
		}
	}
}
