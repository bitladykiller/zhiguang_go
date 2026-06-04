# Go 后端结构标准化重构设计

## 1. 背景

当前项目整体采用 `cmd + internal + pkg` 的 Go 常见组织方式，这个大方向没有问题。

但是后端内部结构存在以下不一致：

- 部分模块按领域分包，但包内职责边界不统一
- 有的模块同时承担 `handler + service` 角色
- 有的模块只有 `handler`，直接访问数据库
- 有的模块有 `repository`，有的模块没有
- `knowpost` 这类核心模块包内文件过多，职责密度偏高
- 启动装配已经集中到 `bootstrap`，但领域模块内部的层次仍然不稳定

这会带来两个问题：

1. 新人进入项目后，很难快速判断“HTTP 入口、业务编排、数据访问”分别在哪里
2. 后续继续加功能时，代码会自然朝“哪里顺手写哪里”继续扩散

## 2. 本次重构目标

本次重构只针对 **Go 后端结构** 做标准化，不改变外部行为。

### 2.1 目标

- 统一后端采用 **Go 领域分包** 结构
- 在每个领域内统一职责边界：
  - `handler`
  - `service`
  - `repository`
  - `model`
  - `dto`
- 保持当前 API 路径、请求响应协议、配置格式、Docker 方案不变
- 保持当前数据库、Redis、Kafka、ES 接入方式不变
- 保持当前启动入口不变

### 2.2 非目标

以下内容不在本次重构范围内：

- 不改 `frontend`
- 不改 `docker-compose.yml`
- 不改配置文件字段结构
- 不改 API 协议
- 不做业务逻辑新增
- 不做数据库 schema 调整
- 不做 LLM / RAG 功能增强

## 3. 设计选择

### 方案 A：最小文件整理

只做轻量文件拆分，例如把 `auth/service.go` 中的 HTTP 处理逻辑拆到 `auth/handler.go`。

优点：

- 风险低
- 改动面小

缺点：

- 只能缓解混乱，不能建立统一规则
- 后续模块仍可能继续分层漂移

### 方案 B：标准 Go 领域分包

保持当前按业务域组织目录的方向，但要求每个领域内部遵循一致的层次结构。

优点：

- 符合 Go 后端常见实践
- 与当前项目现状最兼容
- 可以在不改外部行为的情况下显著提高可读性和可维护性

缺点：

- 需要系统地迁移文件、重命名和修引用

### 方案 C：重型 Clean Architecture

进一步拆为 `domain / application / infrastructure / interfaces`。

优点：

- 理论边界最强

缺点：

- 对当前项目过重
- 容易为了结构而结构
- 改动成本与收益不匹配

### 结论

采用 **方案 B：标准 Go 领域分包**。

## 4. 目标结构

### 4.1 顶层结构

```text
cmd/
  server/

internal/
  bootstrap/
  database/
  messaging/
  server/
  cache/

  auth/
  knowpost/
  relation/
  search/
  counter/
  profile/
  storage/
  llm/
  user/

pkg/
  errcode/
  middleware/
  response/
```

### 4.2 领域包内部标准结构

并不是每个领域都必须机械地拥有完全相同的文件数量，但职责必须统一。

标准模板如下：

```text
internal/<domain>/
  handler.go
  service.go
  repository.go
  model.go
  dto.go
```

如果某个领域确实需要附加能力，可允许增加：

- `worker.go`
- `cache.go`
- `provider.go`
- `helper.go`
- `id.go`

但这些扩展文件必须是补充，不得替代主职责边界。

## 5. 各层职责定义

### 5.1 handler

相当于 Java 里的 `controller`。

职责：

- 注册路由
- 解析 path / query / body
- 从上下文提取登录身份
- 调用 service
- 映射响应和错误

禁止：

- 直接写 SQL
- 执行业务事务
- 编排复杂业务规则

### 5.2 service

职责：

- 负责业务规则与业务编排
- 管理事务边界
- 协调 repository、缓存、消息队列、搜索索引等依赖

禁止：

- 直接耦合 HTTP 请求对象
- 承担路由注册职责

### 5.3 repository

职责：

- 负责持久层读写
- 管理 SQL、数据映射、批量查询、分页查询

说明：

- 当前项目已迁移到 `sqlx`
- `repository` 是数据库访问的标准出口

### 5.4 model

职责：

- 领域模型
- 数据库存储结构
- 必要的投影结构

### 5.5 dto

职责：

- 请求体和响应体
- 只服务于传输协议

说明：

- `dto` 不承担业务逻辑

## 6. 各模块重构目标

### 6.1 auth

当前问题：

- `service.go` 同时承担路由注册、HTTP 处理和业务逻辑

目标：

- 拆成 `handler.go + service.go + repository.go + model.go + dto.go`
- `JwtService`、`VerificationService`、`RefreshTokenStore` 保留在领域内

建议结构：

```text
internal/auth/
  handler.go
  service.go
  repository.go
  model.go
  dto.go
  jwt.go
  verification.go
  store.go
```

### 6.2 knowpost

当前问题：

- 领域很核心，文件已经偏多
- `service` 相关能力被拆成多个文件，但命名和层次仍然不够规整

目标：

- 保留领域内拆分，但按职责重新命名和收敛
- 把“HTTP 入口”和“业务读写编排”彻底分开

建议结构：

```text
internal/knowpost/
  handler.go
  service.go
  repository.go
  model.go
  dto.go
  feed_service.go
  detail_service.go
  cache.go
  helper.go
  id.go
```

### 6.3 relation

当前问题：

- 结构比 `auth` 更清晰，但 DTO 归属不够显式

目标：

- 补齐 `dto.go`
- 保持 `handler / service / repository / model`

### 6.4 search

当前问题：

- `service.go` 和 `outbox_sync.go` 边界大体合理

目标：

- 明确把 `outbox_sync.go` 作为 worker 类文件处理
- 统一命名为 `worker.go` 或 `outbox_worker.go`

### 6.5 counter

当前问题：

- 目前 `handler / service / model / kafka` 基本已成型

目标：

- 评估是否需要补 `repository.go`
- 如果当前不访问数据库，可保持不加，但目录职责说明要清楚

### 6.6 profile

当前问题：

- 目前只有 `handler.go`
- 并且直接访问数据库

目标：

- 补齐 `service.go + repository.go + model.go + dto.go`
- 不再让 handler 直接操作数据库

### 6.7 storage

当前问题：

- 结构接近可接受状态

目标：

- 明确 `handler / service / model`

### 6.8 llm

当前问题：

- 以服务类为主，目前结构较轻

目标：

- 保持领域包，但明确 DTO 与 handler 边界

### 6.9 user

当前问题：

- 当前只有 `repository.go`
- 其角色更像 `auth/profile` 的共享用户存储层

目标：

- 若继续保留，应把它定义为共享用户仓储领域
- 避免未来既有 `auth/repository` 又有 `user/repository` 重复建模

说明：

- 这里需要在实施前做一次引用核对，确定 `user` 是保留还是并入 `auth/profile`

## 7. 基础设施层约束

以下目录继续保留为基础设施或应用骨架，不做领域化拆分：

- `internal/bootstrap`
- `internal/database`
- `internal/messaging`
- `internal/server`
- `internal/cache`

职责要求：

- 不放业务规则
- 只放初始化、基础设施连接、跨领域底层能力

## 8. pkg 的使用原则

当前 `pkg` 里主要有：

- `config`
- `errcode`
- `middleware`
- `response`

本次重构原则：

- `pkg` 只保留跨领域通用能力
- 如果某部分只服务当前后端工程、没有复用价值，后续可再评估是否迁回 `internal`

本次结构重构中，先不主动移动 `pkg`，避免改动过宽。

## 9. 实施策略

本次重构应分阶段进行，避免一次性大搬迁导致行为回归。

### 阶段 1：建立标准骨架

- 为缺失模块补齐标准文件
- 先不做大规模逻辑变更
- 只做“职责搬迁”和“命名收敛”

### 阶段 2：迁移混合职责

- `auth` 从“service 兼 handler”拆开
- `profile` 从“handler 直连 DB”改为三层
- `knowpost` 命名与文件职责收敛

### 阶段 3：统一命名与装配

- 更新 `bootstrap` 的装配引用
- 统一构造函数命名和文件命名
- 更新 README 中的结构说明

## 10. 风险与控制

### 风险 1：文件迁移后引用断裂

控制：

- 每个阶段完成后都跑 `gofmt`
- 跑 `go test ./...`
- 跑 `go vet ./...`

### 风险 2：职责迁移时夹带行为变化

控制：

- 本次重构原则是“只整理结构，不改变对外行为”
- 所有逻辑迁移都应保持输入输出一致

### 风险 3：过度追求对称结构

控制：

- 允许个别轻量模块不强行塞满全部文件
- 但职责边界必须一致

## 11. 验收标准

重构完成后应满足：

- 后端仍采用 Go 领域分包
- 每个核心领域的边界清晰
- `handler/service/repository/model/dto` 角色明确
- `handler` 不再直接访问数据库
- `service` 不再承担 HTTP 入口职责
- 项目可以通过：
  - `gofmt`
  - `go test ./...`
  - `go vet ./...`

## 12. 本次执行范围结论

本次重构只处理 **Go 后端结构标准化**，不处理：

- 前端项目结构
- Docker 目录结构
- 配置协议
- API 协议
- 基础设施选型
