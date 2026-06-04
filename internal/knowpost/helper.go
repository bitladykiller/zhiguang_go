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

// parseStringArray 将 JSON 字符串数组反序列化为 Go []string。
//
// 功能：数据库中 tags 和 img_urls 字段以 JSON 字符串数组格式存储
// （如 `["tag1","tag2"]`），此函数将其反序列化为 Go 的字符串切片。
//
// 参数：
//   - jsonStr: *string，指向 JSON 字符串的可选指针。nil 表示没有数据。
//
// 返回值：[]string，解析后的字符串切片。
//   输入为 nil 或 JSON 格式非法时返回空切片（非 nil），以便调用方可以直接 range 迭代。
//
// 设计决策：返回空切片而非 nil
//   调用方（如 Feed 列表构造）通常会 range 遍历返回值。如果返回 nil，
//   range 虽然也能正常工作，但序列化到 JSON 时 nil 会被编码为 null，
//   而空切片 []string{} 会被编码为 []。统一返回空切片可以确保 JSON 输出的一致性。
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
//
// 功能：将 Go 值（通常是切片或 map）转换为 JSON 字符串，
// 用于存储到数据库的 JSON 类型字段中。
//
// 参数：
//   - v: interface{}，要序列化的值。
//
// 返回值：string，JSON 字符串。序列化失败时返回空字符串（不会 panic）。
func toJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// strPtr 将非空字符串转为 *string 指针。
//
// 功能：用于将空字符串映射为 nil 指针，便于传给数据库驱动或其他期望指针类型的函数。
// 空字符串对应数据库中的 NULL，非空字符串对应指向该字符串的指针。
//
// 参数：
//   - s: string，源字符串。
//
// 返回值：*string，s 非空时返回 &s，s 为空时返回 nil。
//
// 典型用途：KnowPost 结构体中，部分可选字段（如 content_object_key）在数据库中可为 NULL，
// 使用 *string 字段可以让零值空字符串映射为 NULL 而非 ''。
func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
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
