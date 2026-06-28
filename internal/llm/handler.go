package llm

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/httputil"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
	"go.uber.org/zap"
)

type LlmHandler struct {
	descSvc DescriptionServiceInterface
	ragSvc  RagQueryServiceInterface
	logger  *zap.Logger
}

func NewLlmHandler(descSvc DescriptionServiceInterface, ragSvc RagQueryServiceInterface, logger *zap.Logger) *LlmHandler {
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
	done := make(chan struct{})

	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				h.logger.Error("ragSvc.Query panicked", zap.Any("panic", r))
				select {
				case streamChan <- fmt.Sprintf("data: {\"error\": \"internal server error\"}\n\n"):
				default:
				}
				select {
				case streamChan <- "data: [DONE]\n\n":
				default:
				}
			}
		}()
		h.ragSvc.Query(ctx, postID, req.Question, streamChan)
	}()

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
