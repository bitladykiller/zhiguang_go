package profile

import (
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

// ProfileHandler 负责处理资料相关接口。
type ProfileHandler struct {
	svc *Service
}

func NewProfileHandler(svc *Service) *ProfileHandler {
	return &ProfileHandler{svc: svc}
}

// RegisterRoutes 注册资料模块路由。
func (h *ProfileHandler) RegisterRoutes(r *gin.RouterGroup) {
	prof := r.Group("/profiles")
	{
		prof.GET("/:id", h.GetProfile)
		prof.PATCH("/:id", h.UpdateProfile)
	}
}

// GetProfile 处理 `GET /profiles/:id`。
func (h *ProfileHandler) GetProfile(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Fail(c, 400, "invalid id")
		return
	}

	user, appErr := h.svc.GetProfile(c.Request.Context(), id)
	if appErr != nil {
		response.Error(c, appErr)
		return
	}

	response.Success(c, user)
}

// UpdateProfile 处理 `PATCH /profiles/:id`。只有资料所有者本人可以修改。
func (h *ProfileHandler) UpdateProfile(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}

	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Fail(c, 400, "invalid id")
		return
	}

	if userID != id {
		response.Error(c, errcode.ErrForbidden)
		return
	}

	var req ProfilePatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request")
		return
	}

	if appErr := h.svc.UpdateProfile(c.Request.Context(), userID, id, &req); appErr != nil {
		response.Error(c, appErr)
		return
	}

	response.Success(c, gin.H{"success": true})
}
