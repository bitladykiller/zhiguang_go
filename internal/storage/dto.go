// storage 包提供阿里云 OSS 对象存储的相关能力。
//
// 核心功能：
//   - 生成客户端直传 OSS 的预签名 PUT URL（避免文件经过业务服务中转），
//     前端拿到 URL 后可直接 PUT 文件到 OSS，上传完成后再调用知文接口
//     告知 objectKey 完成业务关联。
//   - 生成限时有效的下载 URL（Presigned Get URL）
//   - 对象的公开访问地址构造（支持自定义 CDN 域名或 OSS 默认域名）
//   - 获取对象元数据（ETag、文件大小），用于确认上传完成后的文件完整性校验。
//
// 使用流程：
//   1. 客户端请求 POST /storage/presign
//   2. 服务端返回预签名 PUT URL + objectKey + publicURL
//   3. 客户端用预签名 URL 直接 PUT 文件到 OSS
//   4. 客户端调用 POST /knowposts/:id/content 传入 objectKey 完成业务关联
package storage

import "time"

// StoragePresignRequest 是 `POST /storage/presign` 的请求体。
// FileName 和 ContentType 为必填，用于生成 OSS object key 和设置 Content-Type。
type StoragePresignRequest struct {
	FileName    string `json:"file_name" binding:"required"`
	ContentType string `json:"content_type" binding:"required"`
	Folder      string `json:"folder"`
}

// StoragePresignResponse 是 `POST /storage/presign` 的响应体。
// UploadURL 是限时有效的预签名 PUT URL，客户端可直接上传文件到此 URL。
// ObjectKey 用于后续业务关联（如知文的内容确认）。
type StoragePresignResponse struct {
	UploadURL string    `json:"upload_url"`
	ObjectKey string    `json:"object_key"`
	PublicURL string    `json:"public_url"`
	ExpireAt  time.Time `json:"expire_at"`
}
