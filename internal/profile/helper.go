package profile

import (
	"time"
)

// parseTimePtr 将 *string 转换为 *time.Time（用于 birthday 字段的转换）。
func parseTimePtr(s *string) *time.Time {
	if s == nil || *s == "" {
		return nil
	}
	t, err := time.Parse("2006-01-02", *s)
	if err != nil {
		return nil
	}
	return &t
}
