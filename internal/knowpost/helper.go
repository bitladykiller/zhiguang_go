package knowpost

import "encoding/json"

// --- [纯工具函数] ---

// publicURL 根据 OSS 配置生成对象的公开访问地址。
// 如果配置了自定义域名（PublicDomain）则优先使用，否则回退到 OSS 默认域名。
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
// 支持的可见性选项：public、followers、school、private、unlisted。
func isValidVisible(v string) bool {
	switch v {
	case "public", "followers", "school", "private", "unlisted":
		return true
	}
	return false
}

// parseStringArray 将 JSON 字符串数组反序列化为 Go []string。
// 若输入为 nil 或无效，返回空切片而非 nil。
// 用于解析数据库中 JSON 格式的 tags 和 img_urls 字段。
func parseStringArray(jsonStr *string) []string {
	if jsonStr == nil {
		return []string{}
	}
	var arr []string
	if err := json.Unmarshal([]byte(*jsonStr), &arr); err != nil {
		return []string{}
	}
	return arr
}

// toJSON 将任意值序列化为 JSON 字符串。
// 序列化失败时返回空字符串。
func toJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// strPtr 将非空字符串转为 *string 指针。
// 空字符串返回 nil，适合用于数据库中可为 NULL 的列。
func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// strVal 安全地解引用 *string 指针，nil 指针返回空字符串。
func strVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
