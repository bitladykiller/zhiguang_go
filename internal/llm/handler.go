package llm

import (
	"fmt"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

// LlmHandler 暴露 AI 相关 HTTP 接口。
type LlmHandler struct {
	descSvc *KnowPostDescriptionService
	ragSvc  *RagQueryService
}

func NewLlmHandler(descSvc *KnowPostDescriptionService, ragSvc *RagQueryService) *LlmHandler {
	return &LlmHandler{descSvc: descSvc, ragSvc: ragSvc}
}

func (h *LlmHandler) RegisterRoutes(r *gin.RouterGroup) {
	llm := r.Group("/knowposts")
	{
		llm.POST("/:id/description/suggest", h.SuggestDescription)
		llm.POST("/:id/rag/query", h.RagQuery)
	}
}

// SuggestDescription 处理 `POST /knowposts/:id/description/suggest`。
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

	desc, err := h.descSvc.SuggestDescription(req.Title, req.Content)
	if err != nil {
		response.Fail(c, 500, err.Error())
		return
	}

	response.Success(c, gin.H{"description": desc})
}

// RagQuery 处理 `POST /knowposts/:id/rag/query`，并通过 SSE 流式返回结果。
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

	// 设置 SSE 响应头
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.WriteHeader(200)

	streamChan := make(chan string, 10)

	go func() {
		h.ragSvc.Query(postID, req.Question, streamChan)
	}()

	flusher, _ := c.Writer.(interface{ Flush() })

	for token := range streamChan {
		fmt.Fprint(c.Writer, token)
		if flusher != nil {
			flusher.Flush()
		}
	}
}
