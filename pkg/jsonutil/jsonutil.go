// Package jsonutil 提供 JSON 相关的通用工具函数。
package jsonutil

import "encoding/json"

// ParseStringArray 将 JSON 字符串数组反序列化为 Go []string。
//
// 参数：
//   - raw: 指向 JSON 字符串的指针，nil 表示无数据
//
// 返回值：解析后的字符串切片。输入为 nil 或 JSON 格式非法时返回空切片。
func ParseStringArray(raw *string) []string {
	if raw == nil {
		return []string{}
	}
	var arr []string
	if err := json.Unmarshal([]byte(*raw), &arr); err != nil {
		return []string{}
	}
	return arr
}

// StrPtr 将非空字符串转为 *string 指针，空字符串返回 nil。
func StrPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
