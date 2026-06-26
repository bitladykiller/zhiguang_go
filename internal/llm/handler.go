package llm

import (
	"fmt"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/httputil"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
	"go.uber.org/zap"
)

type LlmHandler struct {
	descSvc DescriptionServiceInterface
	ragSvc  RagQueryServiceInterface
}

func NewLlmHandler(descSvc DescriptionServiceInterface, ragSvc RagQueryServiceInterface) *LlmHandler {
	return &LlmHandler{descSvc: descSvc, ragSvc: ragSvc}
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
		response.Fail(c, 401, "unauthorized")
		return
	}
	if h.descSvc == nil {
		response.Fail(c, 503, "llm description service is unavailable")
		return
	}

	var req struct {
		Title   string `json:"title" binding:"required"`
		Content string `json:"content" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request")
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
		response.Fail(c, 401, "unauthorized")
		return
	}
	if h.ragSvc == nil {
		response.Fail(c, 503, "rag query service is unavailable")
		return
	}

	postID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Fail(c, 400, "invalid post id")
		return
	}

	var req struct {
		Question string `json:"question" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request")
		return
	}

	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.WriteHeader(200)

	ctx := c.Request.Context()
	streamChan := make(chan string, 10)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				zap.L().Error("ragSvc.Query panicked", zap.Any("panic", r))
				select {
				case streamChan <- fmt.Sprintf("data: {\"error\": \"internal server error\"}\n\n"):
				default:
				}
				select {
				case streamChan <- "data: [DONE]\n\n":
				default:
				}
				close(streamChan)
			}
		}()
		h.ragSvc.Query(ctx, postID, req.Question, streamChan)
	}()

	flusher, _ := c.Writer.(interface{ Flush() })

	for token := range streamChan {
		fmt.Fprint(c.Writer, token)
		if flusher != nil {
			flusher.Flush()
		}
	}
}
