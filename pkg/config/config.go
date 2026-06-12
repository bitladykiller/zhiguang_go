// Package config 提供基于 YAML 的配置加载能力。
// 所有配置会在启动时通过 LoadConfig() 一次性读取并反序列化到 Config 结构体，
// 再通过应用装配流程传递给各个服务模块。
//
// 配置设计原则：
//   - 所有配置字段都定义了 yaml tag，与 config.yaml / config-local.yaml 一一对应。
//   - 可选依赖（搜索、LLM、OSS）配置不完整时不会阻止服务启动，
//     而是由调用方自行检测并降级（返回 503）。
//   - itoa 不使用 strconv.Itoa 是为了最小化启动依赖链。
//
// 使用方式：
//
//	cfg, err := config.LoadConfig("config/config-local.yaml")
package config
