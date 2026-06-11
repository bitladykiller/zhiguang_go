package storage

import "time"

// StorageUseCase 定义存储 HTTP 层依赖的业务接口。
//
// HTTP 层只关心对象 key、上传签名和公网 URL 三件事，
// 不应该感知底层是 OSS、S3 还是其他对象存储实现。
type StorageUseCase interface {
	GenerateObjectKey(folder, fileName string) string
	GeneratePresignedPutURL(objectKey string, expiry time.Duration) (string, error)
	PublicURL(objectKey string) string
}
