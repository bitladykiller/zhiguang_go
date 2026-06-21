// Package storage 提供对象存储的抽象接口和 OSS 实现。
//
// ObjectStorage 接口定义了对象存储的最小操作集：
//   - 生成预签名上传/下载 URL
//   - 生成唯一对象键
//   - 获取公开访问 URL
//   - 获取预签名过期时间
//
// 当前实现：OssStorageService（阿里云 OSS）
// 未来可扩展：MinioStorageService、S3StorageService 等
package storage

import "time"

// ObjectStorage 定义对象存储服务的抽象接口。
//
// 设计目的：
//   - 解耦业务代码与具体的云存储 SDK（当前为阿里云 OSS）
//   - 支持未来切换到 MinIO、AWS S3、GCS 等存储后端
//   - 便于单元测试（可注入 mock 实现）
type ObjectStorage interface {
	GeneratePresignedPutURL(objectKey string, expiry time.Duration) (string, error)
	GeneratePresignedGetURL(objectKey string, expiry time.Duration) (string, error)
	GenerateObjectKey(folder, fileName string) string
	PublicURL(objectKey string) string
	PresignExpiry() time.Duration
}

// StorageServiceInterface 是 ObjectStorage 的类型别名，保持向后兼容。
//
// Deprecated: 请直接使用 ObjectStorage 接口。
type StorageServiceInterface = ObjectStorage
