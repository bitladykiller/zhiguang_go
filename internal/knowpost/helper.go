package knowpost

import "encoding/json"

// --- [纯工具函数] --- //

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

func isValidVisible(v string) bool {
	switch v {
	case "public", "followers", "school", "private", "unlisted":
		return true
	}
	return false
}

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

func toJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func strVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
