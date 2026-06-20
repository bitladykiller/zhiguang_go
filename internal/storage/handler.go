package storage

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

// StorageHandler 暴露 OSS 存储相关 HTTP 接口。
type StorageHandler struct {
	svc *OssStorageService
}

// NewStorageHandler 创建 OSS 存储处理器实例。
//
// 功能：将 OSS 存储服务注入 HTTP 处理器。
//
// 参数：
//   - svc: *OssStorageService，OSS 存储服务实例。可能为 nil（配置不完整时）。
//
// 返回值：*StorageHandler，创建好的处理器实例。
func NewStorageHandler(svc *OssStorageService) *StorageHandler {
	return &StorageHandler{svc: svc}
}

// RegisterRoutes 在给定的路由组下注册 OSS 存储相关 HTTP 接口。
//
// 注册的端点：
//   - POST /storage/presign：生成供客户端直传 OSS 的预签名 URL
//
// 说明：
//   所有存储接口都需要 JWT 登录认证。
func (h *StorageHandler) RegisterRoutes(r *gin.RouterGroup) {
	st := r.Group("/storage")
	{
		st.POST("/presign", h.Presign)
	}
}

// Presign 处理 POST /storage/presign，生成 OSS 直传预签名 URL。
//
// 功能：客户端先将文件上传到业务服务、再由业务服务转发到 OSS 的模式会浪费服务器带宽。
// 预签名 URL 允许客户端直接向 OSS 上传文件，服务端只负责生成凭证。
//
// 请求体：StoragePresignRequest，包含文件夹（folder）和文件名（fileName）。
//
// 响应体：StoragePresignResponse，包含：
//   - UploadURL: 预签名 PUT URL（10 分钟内有效）
//   - ObjectKey: 生成的 OSS 对象键
//   - PublicURL: 对象的公开访问地址
//   - ExpireAt: 预签名 URL 的过期时间
//
// 生成流程：
//  1. 通过 GenerateObjectKey 生成唯一 object key（格式：{folder}/{uuid}_{fileName}）。
//  2. 调用 GeneratePresignedPutURL 生成有效期 10 分钟的预签名 PUT URL。
//  3. 客户端收到 URL 后，直接向该 URL 发送 PUT 请求上传文件。
//  4. 上传完成后，客户端调用知文的 ConfirmContent 接口通知服务端。
//
// 边界情况：
//   - 用户未登录：返回 401 Unauthorized。
//   - svc 为 nil（OSS 配置不完整）：返回 503 Service Unavailable。
//   - 请求体 JSON 解析失败：返回 400 Bad Request。
//   - 预签名 URL 生成失败（OSS SDK 内部错误）：返回 500。
func (h *StorageHandler) Presign(c *gin.Context) {
	_, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	if h.svc == nil {
		response.Fail(c, 503, "storage service is unavailable")
		return
	}

	var req StoragePresignRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request: "+err.Error())
		return
	}

	objectKey := h.svc.GenerateObjectKey(req.Folder, req.FileName)
	expiry := h.svc.PresignExpiry()

	uploadURL, err := h.svc.GeneratePresignedPutURL(objectKey, expiry)
	if err != nil {
		response.Fail(c, 500, "failed to generate upload URL: "+err.Error())
		return
	}

	response.Created(c, StoragePresignResponse{
		UploadURL: uploadURL,
		ObjectKey: objectKey,
		PublicURL: h.svc.PublicURL(objectKey),
		ExpireAt:  time.Now().Add(expiry),
	})
}
