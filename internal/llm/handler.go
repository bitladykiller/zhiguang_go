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

// NewLlmHandler 创建 LLM 处理器实例。
//
// descSvc 和 ragSvc 可能为 nil（配置不完整时降级），
// handler 会在调用前检查 nil 并返回 503。
func NewLlmHandler(descSvc *KnowPostDescriptionService, ragSvc *RagQueryService) *LlmHandler {
	return &LlmHandler{descSvc: descSvc, ragSvc: ragSvc}
}

// RegisterRoutes 在给定的路由组下注册 LLM 相关 HTTP 接口。
//
// 参数:
//   - r: Gin 路由组（通常是 /api/v1 下的子路由组）
//
// 注册的端点:
//   - POST /knowposts/:id/description/suggest: AI 摘要生成
//   - POST /knowposts/:id/rag/query: RAG 流式问答（SSE）
//
// 说明:
//   所有接口都需要 JWT 登录认证。
//   路由路径与 Java 版 zhiguang_be 保持一致，确保前端无需区分后端语言实现。
func (h *LlmHandler) RegisterRoutes(r *gin.RouterGroup) {
	llm := r.Group("/knowposts")
	{
		llm.POST("/:id/description/suggest", h.SuggestDescription)
		llm.POST("/:id/rag/query", h.RagQuery)
	}
}

// SuggestDescription 处理 POST /knowposts/:id/description/suggest。
//
// 功能：调用 DeepSeek API 为知文生成简洁的中文摘要。
// 如果摘要服务未初始化（配置不完整），返回 503。
//
// 请求体：{"title": "...", "content": "..."}
// 响应体：{"description": "生成的摘要文本"}
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

// RagQuery 处理 `POST /knowposts/:id/rag/query`，通过 SSE 流式返回 RAG 问答结果。
//
// 请求参数:
//   - id (路径参数): 知文 ID
//   - question (JSON 请求体): 用户问题（必填）
//
// 响应:
//   使用 SSE (Server-Sent Events) 协议流式返回：
//   - Content-Type: text/event-stream
//   - Cache-Control: no-cache
//   - Connection: keep-alive
//
// 处理流程:
//  1. 校验用户登录状态（401 未登录）
//  2. 校验 ragSvc 是否可用（503 服务未就绪）
//  3. 解析路径参数和请求体
//  4. 设置 SSE 响应头
//  5. 启动 goroutine 调用 ragSvc.Query 生成流式 token
//  6. 从 channel 读取 token 并逐段 Flush 到 HTTP ResponseWriter
//
// 关键调用说明:
//   - c.Writer.(interface{ Flush() }): http.Flusher 接口的类型断言。
//     Gin 的 ResponseWriter 包装了 http.ResponseController，实现了 Flush 方法。
//     Flush 将缓冲区中的数据立即发送到客户端，是 SSE 实现的核心机制。
//     如果断言失败（极少见），flusher 为 nil，SSE 仍能工作但不会实时推送。
//
// 边界情况:
//   - svc 为 nil 时返回 503
//   - postID 解析失败时返回 400
//   - question 为空或缺少时返回 400
//   - 客户端断开连接时 channel 读操作仍正常完成（由 ragSvc.Query 负责检测 ctx）
//   - streamChan 使用带缓冲 channel（容量 10），避免 goroutine 阻塞影响生成速度
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
