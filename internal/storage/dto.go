// storage 包提供 OSS 对象存储相关的数据结构，
// 包括预签名直传/直下载 URL 以及公开访问地址场景下的请求响应模型。
package storage

import "time"

// StoragePresignRequest 是 `POST /storage/presign` 的请求体。
type StoragePresignRequest struct {
	FileName    string `json:"file_name" binding:"required"`
	ContentType string `json:"content_type" binding:"required"`
	Folder      string `json:"folder"`
}

// StoragePresignResponse 是 `POST /storage/presign` 的响应体。
type StoragePresignResponse struct {
	UploadURL string    `json:"upload_url"`
	ObjectKey string    `json:"object_key"`
	PublicURL string    `json:"public_url"`
	ExpireAt  time.Time `json:"expire_at"`
}
