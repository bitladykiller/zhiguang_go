# 项目架构说明

这份文档描述当前 Go 后端的主要分层、模块边界和关键异步链路，重点不是列目录，而是解释为什么项目现在这样组织。

## 1. 总体结构

项目采用标准 Go 后端常见的 `cmd + internal + pkg + docs` 布局：

- `cmd/server`
  - 程序入口，只负责读取配置路径并调用 `bootstrap.InitializeApp`
- `internal/bootstrap`
  - 顶层装配层，负责创建基础设施、构建各领域模块、收集后台 runner
- `internal/<domain>`
  - 按业务域组织代码，例如 `auth`、`knowpost`、`counter`、`relation`、`search`
- `internal/server`
  - HTTP 路由、应用生命周期、后台 runner 管理
- `pkg`
  - 可复用基础组件，例如配置加载、分布式锁、错误码
- `docs`
  - 架构和设计文档

整体依赖方向保持为：

1. `cmd` 只依赖 `bootstrap`
2. `bootstrap` 依赖各领域和基础设施
3. `handler` 依赖 `ports`
4. `service` 依赖 repository、cache、消息组件
5. 底层组件不反向依赖 HTTP 层

## 2. bootstrap 顶层装配

`internal/bootstrap` 现在是应用装配的唯一入口。

拆分后的职责如下：

- `app.go`
  - 负责顶层编排顺序
- `infra.go`
  - 创建 MySQL、Redis、Kafka writer、本地缓存、雪花 ID 生成器
- `*_wiring.go`
  - 分领域构建模块
- `optional.go`
  - 统一处理 ES、LLM、OSS 这类可选能力的降级逻辑

这样做的收益是：

1. 新增领域时不需要继续往单个超大初始化函数里堆代码
2. 可选依赖的降级逻辑不会散落到业务层
3. 生命周期注册点统一，后台任务不会隐式自启
4. 跨领域依赖在构造期一次性注入，不再依赖 `New(...)` 之后再 `SetXxx(...)`

## 3. 分层与接口边界

当前领域层基本遵循：

- `handler`
  - 负责 HTTP 协议转换、参数解析、调用 use case、写响应
- `service`
  - 负责业务规则、事务边界、缓存策略、异步链路编排
- `repository`
  - 负责 MySQL / Redis 等数据访问
- `ports`
  - 负责给上层暴露“足够小”的接口边界

### 为什么增加 `ports.go`

之前 handler 直接依赖具体 service，会导致三个问题：

1. handler 测试必须构造完整 service 依赖链
2. service 一旦拆分，HTTP 层很容易被迫一起改签名
3. 更容易出现跨领域直接依赖具体实现的耦合

现在改成 handler 依赖接口后，接口遵循两个原则：

1. 面向使用场景，而不是面向底层实现
2. 尽量窄，只暴露当前 handler 真的需要的方法

例如：

- `knowpost.KnowPostUseCase`
  - 只覆盖写路径和详情页
- `knowpost.KnowPostFeedUseCase`
  - 单独覆盖 Feed 读取
- `search.SearchUseCase`
  - 只暴露搜索和建议词查询

这更符合 Go 社区常见的“小接口、按使用方定义”的风格。

### 为什么同时引入 `Deps` / `Config` 构造参数结构体

除了 handler 依赖接口，当前几个核心 service 还统一做了两件事：

1. 超过 4~5 个参数的构造函数改成 `Deps` / `Config` 结构体
2. 原先靠 setter 做的后置依赖注入，改成构造期注入

原因很直接：

1. bootstrap 里不再存在“先 New，再按顺序补依赖”的隐式约束
2. service 创建完成后就是完整可用状态，不会出现半初始化对象
3. 调用点比长参数列表更可读，也更不容易把多个同类型参数传错位置

## 4. knowpost 缓存设计

`knowpost` 的详情页采用三级读取链路：

1. L1：进程内 `freecache`
2. L2：Redis
3. L3：MySQL

### 为什么不是简单双删

项目已经不是单实例假设，只删当前实例的 L1 不够。当前做法是：

1. 写路径双删时删除当前实例的 L1 和 Redis L2
2. 同时递增 Redis 中的 `detail version`
3. 读路径使用带版本号的详情 key

这样即使其他实例本地 L1 里还残留旧值，也不会继续命中新版本 key，从而规避多实例 L1 不一致导致的短暂脏读。

另外，详情页回源使用 Redis 分布式锁和看门狗续约，避免热点 key 同时穿透到数据库。

## 5. 计数模块设计

计数模块当前把“状态正确性”和“读路径性能”拆开处理。

### 5.1 事实来源

- 用户维度状态以 Redis 位图和集合语义为准
- `cnt:*` 是面向读路径的聚合快照

### 5.2 写路径

请求到达后：

1. 先根据当前状态判断本次操作是否真的发生变化
2. 如果没有变化，直接返回
3. 如果发生变化，生成计数增量消息写入 Kafka

### 5.3 MQ 消费聚合

`internal/counter/aggregation_consumer.go` 在消费端做批量聚合：

1. 按分区拉取消息
2. 先在进程内折叠同实体同指标的 delta
3. 达到批次大小或时间窗口后批量 flush 到 Redis `cnt:*`
4. 成功后再推进应用水位线并提交 Kafka offset

### 5.4 失败补偿

如果 flush 或 apply 连续失败：

1. 失败消息会记录到 MySQL 失败表
2. 后台补偿 worker 再按失败记录重试

这意味着计数链路当前采用的是“性能优先，允许异步补偿”的策略，而不是强一致事务消息。

### 5.5 epoch fence

`like/fav` 这条链路现在额外加了一层实体级 `epoch` fence：

1. `toggle` 在 Lua 中会检查实体的 rebuild 锁；如果正在 rebuild，会短暂等待后重试
2. 每条 `CounterEvent` 和失败任务都会携带实体当前 `epoch`
3. `rebuild` 在持有实体锁时会先 bump `active_epoch`，再基于 bitmap 真值重建 SDS
4. consumer 和 failure worker 只处理当前 `epoch` 的 `like/fav` 事件/任务，旧 `epoch` 直接丢弃或标记完成

这样可以保留 `publish=补发原始 delta` 的语义，同时避免 `rebuild` 后旧 delta 晚到再次污染快照。

## 6. Outbox + Canal + Kafka 链路

当前异步同步链路采用：

`MySQL 事务内写 outbox -> Canal 订阅 binlog -> Kafka -> 各领域 consumer`

这样做的核心目的有三个：

1. 避免业务事务提交成功但异步事件没有发出去
2. 避免应用层手工双写带来一致性问题
3. 让搜索投影、关系投影等派生链路从主写路径解耦

### 为什么消费端改成“分区 + 水位线”

`search.OutboxConsumer` 和 `relation.OutboxConsumer` 当前的主幂等策略不是简单事件 ID 去重，而是：

1. 以 `consumer-group + topic + partition` 为作用域
2. 在 Redis 记录“当前已成功处理到的最大 offset”
3. 只有副作用成功后才推进这个水位线

这样做可以直接覆盖 Kafka 最常见的重复投递来源：

- 副作用成功，但 `CommitMessages` 失败
- consumer 重启后重新拉到同一条消息

因为 Kafka 在同一 partition 内天然有序，所以共享水位线比全局事件去重更贴近底层投递模型。

## 7. 应用生命周期

`internal/server/app.go` 现在统一托管：

- HTTP server
- background runners
- cleanup hooks

启动和关闭顺序是：

1. 创建根 `context.Context`
2. 启动所有后台 runner
3. 启动 HTTP 服务
4. 收到退出信号后取消根上下文
5. 优雅关闭 HTTP server
6. 等待后台 runner 退出
7. 执行资源清理

这比“Gin 退出了，但后台 goroutine 还在跑”的模式更适合容器化部署和优雅停机。

## 8. 可选能力降级

当前把这些能力视为可选依赖：

- Elasticsearch 搜索
- LLM 摘要 / RAG
- OSS 对象存储

处理原则不是“配置不完整就启动失败”，而是：

1. bootstrap 创建失败时记录告警日志
2. 对应能力返回 `nil` use case
3. handler 层统一对外返回 `503`

这样主站核心能力不会被这些扩展能力拖垮。

## 9. 配置与启动期校验

`pkg/config/config.go` 现在不仅负责 YAML 反序列化，还负责：

- 默认值填充
- 启动期参数校验

校验原则是：

1. 核心链路必须完整，否则启动失败
2. 可选能力允许缺失，由 bootstrap 层统一降级

这能把很多运行中才暴露的问题前移到启动阶段。

## 10. 后续扩展建议

如果继续扩展这个项目，建议保持这些约束：

1. 新增领域优先增加新的 `*_wiring.go`
2. handler 继续依赖窄接口，不回退成直接依赖具体 service
3. 新的异步消费者优先接入统一生命周期，不要自行 `go func()`
4. 新的缓存策略先讲一致性边界，再谈命中率
5. 对 Kafka 消费幂等，先定义“幂等作用域”，再选水位线、唯一键或业务去重

当前项目已经从“能跑的单体后端”逐步收敛为“模块边界清晰、异步链路可解释、生命周期统一管理”的 Go 服务结构，后续优化建议继续沿着这个方向推进。

## 11. 工程质量基线

这轮结构改造之后，最容易被后续重构破坏的点不是普通 CRUD，而是这些“框架级行为”：

1. 启动期配置默认值和校验是否还成立
2. HTTP 服务退出时后台 runner 是否会被统一取消
3. 异步消费者的幂等边界是否还正确
4. 多级缓存的一致性策略是否还保持原约束

因此当前建议把下面几个包视为关键回归面：

- `pkg/config`
  - 启动期默认值和校验
- `internal/server`
  - 生命周期、runner 收敛、cleanup 执行
- `internal/outbox`
  - `partition + watermark` 幂等
- `internal/counter`
  - 停机前批量 flush
- `internal/knowpost`
  - 版本化缓存 key
- `internal/search` / `internal/relation`
  - outbox 畸形消息与兜底路径

本地统一验证入口保持为：

```bash
make lint
make check
```

其中 `make check` 会顺序执行 `go vet`、`golangci-lint` 和全量 `go test ./... -cover`，这应该成为后续所有结构性修改的最低交付要求。
