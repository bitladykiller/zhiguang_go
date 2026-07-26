package contextutil

import "context"

// traceIDKey 是 request context 中 trace id 的私有类型键（避免字符串键碰撞）。
type traceIDKey struct{}

// WithTraceID 把 trace id 注入 context。
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey{}, traceID)
}

// TraceIDFrom 取出 trace id；不存在时返回空串。
//
// WHY 放在 contextutil 而不是 middleware：
//
//	写入方（trace 中间件）与读取方（审计、下游日志）分属不同层，
//	键定义放在两者共同的最小依赖上，避免 pkg/audit 为了一个键去 import 整个 gin 中间件栈。
//	此前审计用 ctx.Value("trace_id") 裸字符串键读取，而中间件只写 gin 自己的
//	c.Set 存储——两个存储从未相通，审计日志的 trace_id 一直是死代码。
func TraceIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(traceIDKey{}).(string); ok {
		return v
	}
	return ""
}
