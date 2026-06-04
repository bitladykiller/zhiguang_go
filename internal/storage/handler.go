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

func NewStorageHandler(svc *OssStorageService) *StorageHandler {
	return &StorageHandler{svc: svc}
}

func (h *StorageHandler) RegisterRoutes(r *gin.RouterGroup) {
	st := r.Group("/storage")
	{
		st.POST("/presign", h.Presign)
	}
}

// Presign 处理 `POST /storage/presign`。
// 它会生成一个供客户端直传 OSS 的预签名 PUT URL。
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
	expiry := 10 * time.Minute

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
