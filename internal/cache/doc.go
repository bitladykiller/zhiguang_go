// Package cache 汇集跨业务域复用的缓存基础组件。
//
// 组件一览（每个组件的详细设计见各自文件的类型注释）：
//
//	Tiered[T]       L1(进程内) → L2(Redis) → 分布式锁回源 的通用读穿编排。
//	                关键不变量：缓存只存 Loader 原始产物，用户态在 Get 返回后叠加，
//	                结构上杜绝「用户维度字段写进共享缓存」一类事故。
//	Versions        「版本号编进缓存键」失效模式的读取端，含进程内短缓存与写侧 Drop。
//	PrefixCache     共享 freecache 实例上的键前缀隔离。
//	HotKeyDetector  本地聚合 + Redis Hash 滑动窗口的跨实例热点识别，
//	                hotkey:active 标记值即热度等级。
//	RedisBloom      第三方 RedisBloom（CF.*）适配层，存在性预判，fail-open。
//
// 归属约定：任何跨业务域复用的缓存工具都放在本包，
// 业务包（knowpost 等）只保留键 schema 与业务编排。
package cache
