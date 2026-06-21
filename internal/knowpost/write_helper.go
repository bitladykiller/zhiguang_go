package knowpost

import "encoding/json"

// --- [纯工具函数] ---

// publicURL 根据 OSS 配置生成对象的公开访问地址。
//
// 功能：根据 ossCfg 中的配置，构造 OSS 对象的公开 URL。
//
// URL 构造规则：
//   - 优先使用 PublicDomain（自定义域名）：https://{domain}/{objectKey}
//   - 回退到 OSS 默认域名：https://{bucket}.{endpoint}/{objectKey}
//
// 边界情况：
//   - PublicDomain 末尾可能带斜杠：函数会去掉末尾的斜杠，确保 URL 格式正确。
//
// 参数：
//   - objectKey: string，OSS 对象键。
//
// 返回值：string，完整的公开访问 URL。
func (s *KnowPostService) publicURL(objectKey string) string {
	if s.ossCfg.PublicDomain != "" {
		domain := s.ossCfg.PublicDomain
		if domain[len(domain)-1] == '/' {
			domain = domain[:len(domain)-1]
		}
		return domain + "/" + objectKey
	}
	return "https://" + s.ossCfg.Bucket + "." + s.ossCfg.Endpoint + "/" + objectKey
}

// isValidVisible 校验可见性设置是否合法。
//
// 功能：检查传入的可见性值是否在预定义的合法选项中。
// 支持的可见性选项：public、followers、school、private、unlisted。
//
// 参数：
//   - v: string，要校验的可见性值。
//
// 返回值：bool，true 表示合法，false 表示不合法。
func isValidVisible(v string) bool {
	switch v {
	case "public", "followers", "school", "private", "unlisted":
		return true
	}
	return false
}

// toJSON 将任意值序列化为 JSON 字符串。
//
// 功能：将 Go 值（通常是切片或 map）转换为 JSON 字符串，
// 用于存储到数据库的 JSON 类型字段中。
//
// 参数：
//   - v: interface{}，要序列化的值。
//
// 返回值：string，JSON 字符串。序列化失败时返回空字符串（不会 panic）。
func toJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// strVal 安全地解引用 *string 指针。
//
// 功能：将 *string 还原为 string，nil 指针返回空字符串。
// 相当于 strPtr 的逆操作。
//
// 参数：
//   - s: *string，字符串指针。
//
// 返回值：string，s 非 nil 时返回 *s，s 为 nil 时返回空字符串。
func strVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
