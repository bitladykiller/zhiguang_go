// counter 包实现点赞、收藏、关注等计数能力。
//
// 当前包内实现按职责拆分为多个文件：
//   - service_core.go: 写路径与服务构造
//   - service_read.go: 读路径与批量查询
//   - service_rebuild.go: SDS 快照重建、失败记录与补偿辅助
//   - service_backoff.go: 重建退避与限流
//   - service_binary.go: SDS 二进制编解码
//   - service_scripts.go: Redis Lua 脚本
//
// 这样拆分的目的是把“读写路径、重建逻辑、脚本定义、基础工具”分开，
// 避免单文件持续膨胀后再次回到难以维护的状态。
package counter
