// Package errcode 定义统一的业务错误码以及 AppError 类型。
//
// 设计思路：
//   - 错误码通过 4xxxx / 5xxxx 号段来区分客户端错误和服务端错误，
//     这样 response.Error() 就可以根据号段推导出正确的 HTTP 状态码。
//   - 它与 Java 版 zhiguang_be 中的 ErrorCode 枚举保持一一对应，
//     确保前后端协议对齐，避免出现客户端看了 Java 版文档却找不到对应 Go 错误码的问题。
//   - 预定义单例错误实例（如 ErrBadRequest、ErrNotFound）可供直接引用；
//     如果需要在单次请求上下文中补充更具体的错误消息，请使用 .WithMsg() 方法创建副本，
//     而不是直接修改全局单例。
package errcode

import "fmt"

// ErrorCode 表示 API 响应中返回的数字错误码。
// 编码规则：
//   - 0     -> 成功
//   - 4xx   -> 客户端错误（参数校验失败、鉴权失败等）
//   - 5xx   -> 服务端错误（内部异常、外部依赖不可用等）
//   - 4xxxx -> 具体的业务错误（如 40901 = 标识已存在、40101 = 凭证无效）
type ErrorCode int

const (
	// —— 基础错误码（与 HTTP 状态码对齐）——

	// CodeSuccess 表示请求处理成功，无任何错误。
	CodeSuccess ErrorCode = 0

	// --- 4xx 客户端错误 ---

	// CodeBadRequest 表示请求参数格式错误或缺少必填字段。
	// 常见场景：JSON 反序列化失败、参数超出合法范围等。
	CodeBadRequest ErrorCode = 400
	// CodeUnauthorized 表示缺少访问令牌或令牌无效/已过期。
	// 常见场景：未提供 Authorization 请求头、Token 签名校验失败等。
	CodeUnauthorized ErrorCode = 401
	// CodeForbidden 表示当前用户没有执行该操作的权限。
	// 常见场景：尝试修改他人的资料、查看非公开内容等。
	CodeForbidden ErrorCode = 403
	// CodeNotFound 表示请求的目标资源不存在。
	// 常见场景：查询的知文 ID 不存在、用户 ID 无效等。
	CodeNotFound ErrorCode = 404
	// CodeConflict 表示请求与当前状态冲突。
	// 常见场景：使用已注册的手机号/邮箱再次注册。
	CodeConflict ErrorCode = 409
	// CodeTooManyRequests 表示操作频率过高，触发了限流保护。
	// 常见场景：频繁发送验证码、短时间内多次尝试登录等。
	CodeTooManyRequests ErrorCode = 429

	// --- 鉴权相关业务错误（5 位错误码，后三位映射到具体的 HTTP 状态码） ---

	// ErrCodeIdentifierExists 表示用户标识（手机号/邮箱）已被注册。
	// 映射到 HTTP 409（Conflict）。
	ErrCodeIdentifierExists ErrorCode = 40901
	// ErrCodeIdentifierNotFound 表示登录时未找到对应的用户标识。
	// 映射到 HTTP 404（Not Found）。
	ErrCodeIdentifierNotFound ErrorCode = 40401
	// ErrCodeInvalidCredentials 表示密码不匹配或验证码错误。
	// 映射到 HTTP 401（Unauthorized）。
	ErrCodeInvalidCredentials ErrorCode = 40101
	// ErrCodeRefreshTokenInvalid 表示刷新令牌已过期、被撤销或格式非法。
	// 映射到 HTTP 401（Unauthorized）。
	ErrCodeRefreshTokenInvalid ErrorCode = 40102
	// ErrCodeTermsNotAccepted 表示用户注册时未同意服务条款。
	// 映射到 HTTP 400（Bad Request）。
	ErrCodeTermsNotAccepted ErrorCode = 40001
	// ErrCodeVerificationNotFound 表示验证码未发送过或已过期。
	// 映射到 HTTP 404（Not Found）。
	ErrCodeVerificationNotFound ErrorCode = 40402
	// ErrCodeVerificationMismatch 表示用户输入的验证码与 Redis 中保存的不匹配。
	// 映射到 HTTP 400（Bad Request）。
	ErrCodeVerificationMismatch ErrorCode = 40002
	// ErrCodeVerificationTooManyAttempts 表示验证码尝试次数超过最大限制。
	// 映射到 HTTP 429（Too Many Requests）。
	ErrCodeVerificationTooManyAttempts ErrorCode = 42901

	// --- 5xx 服务端错误 ---

	// CodeInternalError 表示未预期的内部服务端错误。
	// 调用方应忽略该错误的具体消息并重试。
	CodeInternalError ErrorCode = 500
)

// AppError 是统一错误类型，同时包含业务错误码和可读错误信息。
// 它实现了 error 接口，并被 response.Error() 用来构造统一格式的 JSON 错误响应。
type AppError struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
}

// Error 实现标准 error 接口。
//
// 功能：返回格式为 "[{code}] {message}" 的字符串，例如 "[404] not found"。
// 这使得 AppError 可以作为 error 类型在函数间传递，并用于日志记录。
//
// 实现 error 接口的意义：
// Go 的错误处理惯例是函数返回 error，统一的 AppError 类型实现了 error 接口，
// 使得业务逻辑可以在服务层之间传递结构化的错误信息（包含业务错误码和可读消息），
// handler 层再从中提取错误码映射为 HTTP 状态码。
//
// 返回值：string，格式化为 "[{Code}] {Message}" 的文本。
func (e *AppError) Error() string {
	return fmt.Sprintf("[%d] %s", e.Code, e.Message)
}

// WithMsg 返回一个携带新错误消息的 AppError 副本。
//
// 功能：在预定义错误实例（如 ErrNotFound、ErrBadRequest）的基础上，
// 创建一个新 AppError 对象，消息替换为自定义内容。
// 这比直接修改全局单例更安全，因为全局单例被多个请求共享。
//
// 为什么是副本而非修改原对象：
//   预定义的错误实例（如 ErrNotFound）是包级变量，多个请求可能同时使用。
//   如果直接修改 Message 字段，会造成并发读写冲突（data race）。
//   WithMsg 创建新对象，是线程安全的。
//
// 参数：
//   - msg: string，自定义的错误消息内容。
//
// 返回值：
//   - *AppError: 新的 AppError 实例，错误码不变，消息替换为 msg。
//
// 使用示例：
//   errcode.ErrNotFound.WithMsg("user id %d not found", userID)
func (e *AppError) WithMsg(msg string) *AppError {
	return &AppError{Code: e.Code, Message: msg}
}

// --- 预定义错误实例 ---
// 这些是常见错误场景下可直接复用的单例。
// 如需补充单次请求上下文，请使用 .WithMsg()，不要直接修改原对象。

var (
	ErrBadRequest      = &AppError{Code: CodeBadRequest, Message: "bad request"}
	ErrUnauthorized    = &AppError{Code: CodeUnauthorized, Message: "unauthorized"}
	ErrForbidden       = &AppError{Code: CodeForbidden, Message: "forbidden"}
	ErrNotFound        = &AppError{Code: CodeNotFound, Message: "not found"}
	ErrInternal        = &AppError{Code: CodeInternalError, Message: "internal server error"}

	ErrIdentifierExists            = &AppError{Code: ErrCodeIdentifierExists, Message: "identifier already exists"}
	ErrIdentifierNotFound          = &AppError{Code: ErrCodeIdentifierNotFound, Message: "identifier not found"}
	ErrInvalidCredentials          = &AppError{Code: ErrCodeInvalidCredentials, Message: "invalid credentials"}
	ErrRefreshTokenInvalid         = &AppError{Code: ErrCodeRefreshTokenInvalid, Message: "invalid refresh token"}
	ErrTermsNotAccepted            = &AppError{Code: ErrCodeTermsNotAccepted, Message: "terms not accepted"}
	ErrVerificationNotFound        = &AppError{Code: ErrCodeVerificationNotFound, Message: "verification code not found"}
	ErrVerificationMismatch        = &AppError{Code: ErrCodeVerificationMismatch, Message: "verification code mismatch"}
	ErrVerificationTooManyAttempts = &AppError{Code: ErrCodeVerificationTooManyAttempts, Message: "too many verification attempts"}
)
