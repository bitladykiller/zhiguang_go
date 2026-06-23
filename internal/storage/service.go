package storage

import (
	"fmt"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/google/uuid"
	"github.com/zhiguang/app/pkg/config"
)

// OssStorageService 管理 OSS 对象存储相关操作，实现 ObjectStorage 接口。
//
// 它提供直传 OSS 的预签名 URL，以及对象公开访问地址的构造能力。
type OssStorageService struct {
	client *oss.Client
	bucket *oss.Bucket
	cfg    *config.OssConfig
}

// 编译期断言：*OssStorageService 实现了 ObjectStorage 接口。
var _ ObjectStorage = (*OssStorageService)(nil)

// NewOssStorageService 创建 OSS 客户端以及 bucket 引用。
//
// 参数:
//   - cfg: OSS 配置（含 Endpoint、AccessKeyID、AccessKeySecret、Bucket 等）
//
// 返回值:
//   - *OssStorageService: 存储服务实例
//   - error: 客户端创建失败或 bucket 获取失败时返回
//
// OSS SDK 调用说明:
//   1. oss.New(endpoint, accessKeyID, accessKeySecret):
//      阿里云 OSS Go SDK 的客户端构造方法。
//      endpoint 格式为 "oss-cn-hangzhou.aliyuncs.com"（不带 https://），
//      SDK 会自动补充协议前缀。支持内网 endpoint（如 "oss-cn-hangzhou-internal.aliyuncs.com"）。
//      如果使用 STS 临时凭证，应使用 oss.NewWithSTS 方法。
//
//   2. client.Bucket(bucketName):
//      通过 Bucket 方法获取 bucket 引用，后续的 SignURL、GetObjectDetailedMeta 等操作
//      都是 Bucket 对象的方法。bucket 引用是轻量级的，多次调用不产生网络开销。
func NewOssStorageService(cfg *config.OssConfig) (*OssStorageService, error) {
	client, err := oss.New(cfg.Endpoint, cfg.AccessKeyID, cfg.AccessKeySecret)
	if err != nil {
		return nil, fmt.Errorf("create oss client: %w", err)
	}

	bucket, err := client.Bucket(cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("get bucket: %w", err)
	}

	return &OssStorageService{client: client, bucket: bucket, cfg: cfg}, nil
}

// GeneratePresignedPutURL 生成一个限时有效的 PUT 预签名 URL。
// 客户端可直接用此 URL 上传文件到 OSS，无需经过业务服务中转。
//
// 参数:
//   - objectKey: OSS 对象完整路径（如 "images/abc123_photo.jpg"）
//   - expiry: URL 有效期时长
//
// 返回值:
//   - string: 预签名 PUT URL（已包含 Signature 查询参数）
//   - error: 签名失败时返回
//
// OSS SDK 调用说明:
//   s.bucket.SignURL(objectKey, oss.HTTPPut, int64(expiry.Seconds())):
//   阿里云 OSS SDK 的 SignURL 方法在 URL 末尾附加 AWSAccessKeyId、Expires、Signature 等查询参数。
//   签名过程使用 HMAC-SHA1 算法，由 SDK 内部自动完成。
//   生成的 URL 可以直接用于 curl、浏览器或任何 HTTP 客户端发起 PUT 请求。
//
// 使用场景:
//   客户端（Web/移动端）请求预签名 URL 后，直接通过 PUT 方法上传文件。
//   这种方式避免了文件数据经过业务服务器，减少了带宽消耗和延迟。
//
// 边界情况:
//   - expiry <= 0 时 SDK 行为不确定，建议传入至少 1 分钟
//   - objectKey 以 "/" 开头或包含特殊字符时，需确认 SDK 是否做 URL 编码
func (s *OssStorageService) GeneratePresignedPutURL(objectKey string, expiry time.Duration) (string, error) {
	return s.bucket.SignURL(objectKey, oss.HTTPPut, int64(expiry.Seconds()))
}

// PublicURL 生成 OSS 对象的公开访问 URL。
// 如果配置了自定义域名（CDN）则优先使用，否则退回 OSS 默认域名。
//
// 参数:
//   - objectKey: OSS 对象完整路径
//
// 返回值:
//   - string: 完整的公开访问 URL
//
// URL 构造规则:
//   - 有自定义域名 (PublicDomain): https://{domain}/{objectKey}
//     例如: https://media.example.com/images/abc123_photo.jpg
//   - 无自定义域名 (默认): https://{bucket}.{endpoint}/{objectKey}
//     例如: https://my-bucket.oss-cn-hangzhou.aliyuncs.com/images/abc123_photo.jpg
//
// 说明:
//   自定义域名通常指向 CDN 加速节点，可以提升图片等静态资源的加载速度。
//   OSS 控制台中需要配置 Bucket 绑定自定义域名并开启 CDN 加速。
//
// 边界情况:
//   - PublicDomain 末尾可能带 "/"，函数通过 TrimRight 统一去除
//   - Bucket 或 Endpoint 配置为空时生成的 URL 不完整，调用方需确保配置正确
func (s *OssStorageService) PublicURL(objectKey string) string {
	if s.cfg.PublicDomain != "" {
		domain := strings.TrimRight(s.cfg.PublicDomain, "/")
		return fmt.Sprintf("https://%s/%s", domain, objectKey)
	}
	return fmt.Sprintf("https://%s.%s/%s", s.cfg.Bucket, s.cfg.Endpoint, objectKey)
}

// GenerateObjectKey 为新上传对象生成一个唯一的 OSS object key。
//
// 参数:
//   - folder: 存储目录（如 "images"、"videos"），为空时使用配置中的默认 Folder 或 "uploads"
//   - fileName: 原始文件名（保留扩展名，如 "photo.jpg"）
//
// 返回值:
//   - string: 生成的 object key，格式为 "{folder}/{uuid}_{fileName}"
//
// 设计原因:
//   - UUID 前缀（取前 8 位）确保同一用户的两次上传不会因同名文件而互相覆盖
//   - 保留原始文件名有助于调试和人工识别
//   - uuid.New().String()[:8] 使用 UUID 的前 8 位十六进制字符，碰撞概率极低
//     对于当前业务场景已足够，无需完整 UUID 或雪花 ID
//
// 注意:
//   uuid.UUID 来自 google/uuid 包，基于 crypto/rand 生成的随机 UUIDv4。
//   .String()[:8] 取了短前缀，若需完整唯一性可改用完整 uuid.New().String()。
func (s *OssStorageService) GenerateObjectKey(folder, fileName string) string {
	f := folder
	if f == "" {
		f = s.cfg.Folder
	}
	if f == "" {
		f = "uploads"
	}
	return fmt.Sprintf("%s/%s_%s", f, uuid.New().String()[:8], fileName)
}

// PresignExpiry 返回预签名 URL 过期时间。
//
// 功能：优先使用配置值，未配置则返回默认值 10 分钟。
func (s *OssStorageService) PresignExpiry() time.Duration {
	if s.cfg.PresignExpiryMs > 0 {
		return time.Duration(s.cfg.PresignExpiryMs) * time.Millisecond
	}
	return 10 * time.Minute
}
