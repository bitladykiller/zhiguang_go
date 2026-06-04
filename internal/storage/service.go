package storage

import (
	"fmt"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/google/uuid"
	"github.com/zhiguang/app/pkg/config"
)

// OssStorageService 管理 OSS 对象存储相关操作。
// 它提供直传 OSS 的预签名 URL，以及对象公开访问地址的构造能力。
type OssStorageService struct {
	client *oss.Client
	bucket *oss.Bucket
	cfg    *config.OssConfig
}

// NewOssStorageService 创建 OSS 客户端以及 bucket 引用。
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

// GeneratePresignedPutURL 生成一个限时有效的 PUT 上传 URL。
// 客户端可直接把文件传到 OSS，而无需先经过业务服务转发。
func (s *OssStorageService) GeneratePresignedPutURL(objectKey string, expiry time.Duration) (string, error) {
	return s.bucket.SignURL(objectKey, oss.HTTPPut, int64(expiry.Seconds()))
}

// GeneratePresignedGetURL 生成一个限时有效的下载 URL。
func (s *OssStorageService) GeneratePresignedGetURL(objectKey string, expiry time.Duration) (string, error) {
	return s.bucket.SignURL(objectKey, oss.HTTPGet, int64(expiry.Seconds()))
}

// PublicURL 生成对象的长期公开访问 URL。
// 如果配置了自定义域名则优先使用，否则退回 OSS 默认域名。
func (s *OssStorageService) PublicURL(objectKey string) string {
	if s.cfg.PublicDomain != "" {
		domain := strings.TrimRight(s.cfg.PublicDomain, "/")
		return fmt.Sprintf("https://%s/%s", domain, objectKey)
	}
	return fmt.Sprintf("https://%s.%s/%s", s.cfg.Bucket, s.cfg.Endpoint, objectKey)
}

// GenerateObjectKey 为新上传对象生成唯一 object key。
// 格式为：`{folder}/{uuid}_{filename}`。
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

// GetObjectMeta 获取对象的 ETag 和内容长度。
func (s *OssStorageService) GetObjectMeta(objectKey string) (etag string, size int64, err error) {
	props, err := s.bucket.GetObjectDetailedMeta(objectKey)
	if err != nil {
		return "", 0, err
	}
	etag = props.Get("ETag")
	contentLength := props.Get("Content-Length")
	if contentLength != "" {
		fmt.Sscanf(contentLength, "%d", &size)
	}
	return etag, size, nil
}
