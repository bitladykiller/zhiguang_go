// llm 包提供一组 AI 能力服务：
//   - KnowPostDescriptionService：通过 DeepSeek API 生成帖子简洁的中文摘要（不超过 50 字）
//   - RagQueryService：执行向量检索并以流式 SSE 方式生成问答结果
//
// 使用方式：
//
//	这些服务在配置不完整时不会阻塞服务启动，而是由调用方判断并返回 503。
//	在 bootstrap 中通过 buildDescriptionService / buildRagQueryService 函数
//	检测配置完整性后创建服务实例或返回 nil。
package llm
