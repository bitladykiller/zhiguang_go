# Go 版 Canal 对齐 Java 实现设计

## 1. 背景

当前 Go 版已经具备：

- 业务事务内写 `outbox`
- 搜索侧通过后台轮询 worker 读取 `outbox`
- 再把变更投影到 Elasticsearch

当前 Java 版并不是简单的“轮询 outbox”，而是走这条链路：

1. 业务事务写 `outbox`
2. `CanalKafkaBridge` 订阅 MySQL binlog 中的 `outbox` 表变更
3. 把 Canal 消息写入 Kafka 主题 `canal-outbox`
4. 下游 Kafka Consumer 分别处理：
   - `relation` 事件
   - `search` 事件

这条链路的关键文件在 Java 项目中分别是：

- `CanalKafkaBridge.java`
- `CanalOutboxConsumer.java`
- `CanalOutboxConsumerSearch.java`
- `OutboxTopics.java`
- `OutboxMessageUtil.java`

因此，如果要“尽量和 Java 项目一样”，Go 版最合理的目标不是直接用 Canal 替代轮询 worker，而是实现同样的：

`Canal -> Kafka -> Consumer`

## 2. 目标

### 2.1 目标

- 让 Go 版 Canal 实现方式尽量贴近 Java 版
- 保留当前事务 outbox 模式
- 用 Canal 订阅 `outbox` 表
- 把 Canal 消息桥接到 Kafka
- 由 Kafka Consumer 驱动搜索索引更新
- 为关系模块保留与 Java 一致的扩展点

### 2.2 非目标

- 不改数据库 schema
- 不改外部 API
- 不改前端
- 不改 Docker 总体结构
- 不在本次顺带补齐 LLM / RAG
- 不直接监听业务表（如 `know_posts` / `following`）

## 3. 方案比较

### 方案 A：按 Java 同构实现 `Canal -> Kafka -> Consumer`

实现链路：

- 事务写 `outbox`
- Canal 订阅 `outbox`
- Go 桥接器写 Kafka
- Kafka Consumer 再执行业务投影

优点：

- 与 Java 版最一致
- 对 `relation`、`search` 的后续扩展最自然
- 更利于后续多消费者订阅同一 outbox 事件流

缺点：

- 比直接轮询更复杂
- 需要新增 Canal 客户端与 Kafka 消费逻辑

### 方案 B：Canal 直接替代轮询 worker

实现链路：

- 事务写 `outbox`
- Canal 订阅 `outbox`
- Go 进程内直接处理并投影到 ES

优点：

- 实现更简单
- 少一层 Kafka

缺点：

- 不符合 Java 版实现
- 后续多个下游消费者会重新变复杂

### 方案 C：保留现有轮询 worker，不实现 Canal

优点：

- 改动最小

缺点：

- 与 Java 版完全不一致
- 现有 `canal` 配置继续是死配置

### 结论

采用 **方案 A：按 Java 同构实现 `Canal -> Kafka -> Consumer`**。

## 4. 目标链路

最终 Go 版链路应为：

1. 业务服务事务写入 `outbox`
2. Canal Server 监听 `zhiguang.outbox`
3. Go `CanalBridge` 从 Canal 拉取 `INSERT/UPDATE`
4. `CanalBridge` 把标准化 JSON 消息发送到 Kafka 主题 `canal-outbox`
5. Kafka Consumer 消费该主题
6. `search` 消费者根据 payload 更新 ES
7. 关系模块未来可接自己的消费者

## 5. 与 Java 版对齐点

### 5.1 对齐点

- 监听表：`outbox`
- Canal 过滤表达式：`zhiguang\.outbox`
- 桥接主题：`canal-outbox`
- 桥接层只做：
  - 拉取 Canal 消息
  - 过滤事件类型
  - 提取 row data
  - 发 Kafka
- 业务消费者再决定如何处理 payload

### 5.2 不完全照抄的部分

Go 版不会逐行复制 Java 的 Spring 生命周期与注解模式，而会使用：

- `server.BackgroundRunner`
- `segmentio/kafka-go`
- 现有 `bootstrap` 装配体系

也就是说：

- **架构一致**
- **运行机制按 Go 习惯实现**

## 6. 目标目录结构

建议新增和调整如下：

```text
internal/
  canal/
    bridge.go
    message.go
    parser.go

  outbox/
    topics.go
    message_util.go

  search/
    handler.go
    service.go
    outbox_consumer.go

  relation/
    handler.go
    service.go
    repository.go
    model.go
    dto.go
    event_processor.go
    outbox_consumer.go
```

说明：

- `internal/canal`
  - 对应 Java 的 `CanalKafkaBridge`
- `internal/outbox`
  - 放主题常量和消息解析工具
  - 对应 Java 的 `OutboxTopics` 与 `OutboxMessageUtil`
- `internal/search/outbox_consumer.go`
  - 对应 Java 的 `CanalOutboxConsumerSearch`

## 7. 组件设计

### 7.1 `internal/outbox/topics.go`

职责：

- 定义 Kafka 主题常量

至少包含：

- `CanalOutboxTopic = "canal-outbox"`

WHY：

- 这是 Java 版明确存在的单独常量文件
- 避免字符串在桥接器和消费者中散落

### 7.2 `internal/outbox/message_util.go`

职责：

- 解析桥接器发到 Kafka 的 JSON 消息
- 只提取合法的 `outbox` 行数组

消息结构与 Java 对齐：

```json
{
  "table": "outbox",
  "type": "INSERT",
  "data": [
    { "payload": "{...}" }
  ]
}
```

WHY：

- Java 版消费者并不是直接消费 payload，而是先通过工具类抽 row
- Go 版也应复用这层解析，避免每个 consumer 自己拆 JSON

### 7.3 `internal/canal/bridge.go`

职责：

- 作为 `BackgroundRunner` 启动
- 连接 Canal Server
- 订阅 `cfg.Canal.Filter`
- 循环调用 `getWithoutAck` 风格的拉取
- 只处理 `INSERT/UPDATE`
- 将标准化 JSON 写入 Kafka `canal-outbox`

行为约束：

- `cfg.Canal.Enabled == false` 时不启动
- 启动失败写日志，不阻塞主服务启动
- 成功处理一个 batch 后 ack
- 解析失败或发送失败时不 ack，该 batch 下次重试

WHY：

- 与 Java 版的“至少一次”语义保持一致

### 7.4 `internal/canal/parser.go`

职责：

- 把 Canal row event 转成标准化 JSON 结构
- 只保留当前需要的列

首版只提取：

- `payload`
- 可选补充：
  - `id`
  - `type`
  - `aggregate_type`
  - `aggregate_id`

建议：

- 首版保持与 Java 一样，最小集只传 `payload`
- 如果 Go 消费端确实需要更多信息，再扩展

### 7.5 `internal/search/outbox_consumer.go`

职责：

- 消费 Kafka 主题 `canal-outbox`
- 用 `outbox/message_util.go` 提取 rows
- 解析 `payload`
- 仅处理 `entity=knowpost`
- 决定执行：
  - upsert
  - soft delete

设计建议：

- 当前已有 `SearchService`
- 新增一个更细的投影接口，例如：
  - `UpsertKnowPost(id uint64) error`
  - `SoftDeleteKnowPost(id uint64) error`

这样实现会更贴近 Java 版 `SearchIndexService`

### 7.6 `internal/relation/outbox_consumer.go`

职责：

- 消费 Kafka 主题 `canal-outbox`
- 用 `outbox/message_util.go` 提取 rows
- 解析 `payload`
- 反序列化为关系事件
- 交给关系事件处理器完成：
  - 粉丝表更新
  - 关注/粉丝缓存更新
  - 关注数/粉丝数变更

需要新增：

- `internal/relation/event_processor.go`
- `internal/relation/outbox_consumer.go`

WHY：

- Java 版当前已经有正式的 relation outbox consumer
- 如果只实现搜索消费者，Go 版仍然和 Java 版不一致
- 关系事件本身就是 outbox 的首要下游，不应继续停留在“预留”

## 8. 与现有 Go 实现的关系

### 8.1 当前已有实现

当前 Go 版已有：

- 事务 outbox 写入
- 轮询 worker：
  - `internal/search/outbox_worker.go`

### 8.2 重构策略

本次不建议让两套消费链路长期共存。

建议分两步：

#### 第一步：并存但受开关控制

- `cfg.Canal.Enabled = false`
  - 启动现有 `OutboxSyncWorker`
- `cfg.Canal.Enabled = true`
  - 启动 `CanalBridge`
  - 启动 `relation` Kafka consumer
  - 启动 `search` Kafka consumer
  - 不再启动 `OutboxSyncWorker`

#### 第二步：验证稳定后移除轮询 worker

只有在新链路稳定后，才删除 `outbox_worker`

WHY：

- 先保留回退路径
- 避免一次性替换导致搜索同步完全不可用
- 同时确保 `relation` 与 `search` 都走同一条 Java 同构链路

## 9. Kafka 侧设计

当前 Go 项目已经有：

- Kafka Writer 工厂
- `counter` 的 Kafka 生产逻辑

需要新增：

- Kafka Reader 的启动与消费循环
- 一个通用的消费者运行器

建议实现方式：

- `internal/messaging/kafka.go` 保留 Reader 工厂
- 在消费者内部自己循环 `ReadMessage` / `FetchMessage + CommitMessages`

语义建议：

- 成功处理后提交 offset
- 处理失败不提交 offset，触发重试

这和 Java 版“手动 ack”语义对齐

## 10. 配置设计

沿用当前已有 `canal` 配置，不改字段名：

- `enabled`
- `host`
- `port`
- `destination`
- `username`
- `password`
- `filter`
- `batch_size`
- `interval_ms`

补充建议：

- 在 `kafka.topics` 下增加：
  - `canal_outbox`

如果你坚持完全对齐 Java 版主题名，也可以直接写死 `canal-outbox` 常量，不从配置读取。

推荐：

- **主题常量固定**
- 不引入额外配置字段

WHY：

- Java 版就是常量
- 主题名属于系统内部契约，不值得过早配置化

## 11. 错误处理与可靠性

### 11.1 CanalBridge

- 连接失败：打 `warn/error`，后台重试
- 空批次：睡眠 `interval_ms`
- 解析失败：整批不 ack
- Kafka 发送失败：整批不 ack

### 11.2 Kafka Consumer

- 解析 Canal 包装消息失败：不提交 offset
- payload 反序列化失败：不提交 offset
- 单条业务处理失败：不提交 offset

WHY：

- Java 版同样依赖“不提交位点 = 重试”
- 先保证一致性，再谈吞吐

## 12. 测试策略

### 12.1 单元测试

- `outbox/message_util.go`
  - 合法 `outbox INSERT`
  - 非 `outbox`
  - 非 `INSERT/UPDATE`
  - 非数组 data
- `canal/parser.go`
  - row 转标准化 JSON
- `search/outbox_consumer.go`
  - 仅处理 `entity=knowpost`
  - `op=delete` 触发软删
  - 其他 op 触发 upsert

### 12.2 集成测试

可先不接真实 Canal Server。

第一阶段重点验证：

- 给 `relation` consumer 喂一条与 Java 相同格式的 Kafka 消息
  - 是否能触发关系事件处理器
- 给 `search` consumer 喂一条与 Java 相同格式的 Kafka 消息
  - 是否能驱动搜索投影正确执行

### 12.3 回归验证

- `cfg.Canal.Enabled = false` 时，老链路仍能正常工作
- `cfg.Canal.Enabled = true` 时，新链路能替代轮询 worker

## 13. 实施顺序

1. 新增 `internal/outbox/topics.go`
2. 新增 `internal/outbox/message_util.go`
3. 抽搜索投影逻辑，减少与轮询 worker 的耦合
4. 新增 `internal/canal/bridge.go`
5. 新增 `internal/relation/event_processor.go`
6. 新增 `internal/relation/outbox_consumer.go`
7. 新增 `internal/search/outbox_consumer.go`
8. `bootstrap` 中实现：
   - Canal 链路
   - relation/search 两类 consumer
   - 轮询 worker
   - 二选一启动
9. 补测试
10. 跑 `gofmt`、`go test ./...`、`go vet ./...`

## 14. 最终结论

要和 Java 版尽量一致，Go 版的正确实现不是：

- “直接用 Canal 替代轮询 worker”

而是：

- “保留 outbox”
- “Canal 监听 outbox”
- “桥接到 Kafka”
- “Kafka consumer 再处理搜索/关系事件”

这是本次设计的核心决策。
