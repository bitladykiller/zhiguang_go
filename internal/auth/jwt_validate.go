package auth

import (
	"fmt"

	"github.com/golang-jwt/jwt/v5"
	"github.com/zhiguang/app/pkg/middleware"
)

// ValidateToken 使用 RS256 公钥校验并解析 JWT 字符串。
//
// 参数：
//   - tokenStr: 完整的 JWT 字符串（形如 "eyJhbGciOiJSUzI1NiIs..."）
//
// 返回值：
//   - middleware.TokenClaims: 解析后的 claims（包含 UserID 和 TokenType）
//   - error: 如果签名校验失败、令牌过期或算法不匹配则返回错误
//
// 函数调用说明：
//   - jwt.ParseWithClaims(tokenStr, &JWTClaims{}, keyFunc):
//     golang-jwt 库的解析函数。
//     第一个参数是 JWT 字符串。
//     第二个参数是自定义 claims 结构体（JWTClaims）的指针，解析后会自动填充。
//     第三个参数是一个 keyFunc 回调，用于返回验证密钥。
//     内部会自动校验：签名（RS256）、过期时间（exp）、签发时间（iat）等标准字段。
//   - token.Method.(*jwt.SigningMethodRSA):
//     类型断言，检查 JWT header 中声明的签名算法是否为 RSA 族算法。
//     如果不是则拒绝，防止攻击者使用 HS256 等对称算法欺骗验证（算法混淆攻击）。
func (s *JWTService) ValidateToken(tokenStr string) (middleware.TokenClaims, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &JWTClaims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return s.publicKey, nil
	})
	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(*JWTClaims); ok && token.Valid {
		return claims, nil
	}

	return nil, fmt.Errorf("invalid token")
}
