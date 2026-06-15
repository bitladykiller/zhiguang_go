# 知光平台 (ZhiGuang) - Go 重构版

知识获取与分享社区后端服务，从 Java Spring Boot 重构为 Go 语言实现。

## 当前状态

- HTTP 服务使用 Gin，入口是 `cmd/server/main.go`
- 依赖装配走共享 bootstrap，入口在 `internal/bootstrap/app.go`
- 本地开发推荐方式：
  依赖服务走 Docker Compose
  Go 应用继续本机运行
- 搜索能力支持全文检索和 completion suggester
- `knowpost` 变更会通过事务内 outbox + Canal/Kafka 消费链路投递到 Elasticsearch
- `knowpost` 的缓存回源使用 Redis 分布式锁 + 手写看门狗续约，避免长尾回源时锁提前过期
- `canal.enabled=true` 时，会切换为与 Java 版一致的 `Canal -> Kafka -> relation/search consumers` 链路
- `canal.enabled=false` 时，不会启动异步 outbox 消费链路
- Kafka 本地环境已调整为 3 broker；`counter-events` 与 `canal-outbox` 主题使用 3 副本并要求 `min.insync.replicas=2`
- `docker-compose.yml` 已包含本地 Canal 服务，默认会订阅 `zhiguang.outbox`
- Canal 配置通过自定义镜像内置，`conf/example` 与 `logs` 使用 Docker 命名卷持久化
- LLM/RAG、OSS 存储在配置不完整时会自动降级为 `503`，不会阻塞服务启动

## 技术栈

| 组件 | Go 实现 |
|------|---------|
| HTTP 框架 | Gin |
| SQL 访问层 | sqlx |
| 本地缓存 | freecache |
| Redis 客户端 | go-redis/v9 |
| 消息队列 | segmentio/kafka-go |
| 搜索引擎 | go-elasticsearch v8 |
| JWT 认证 | golang-jwt/v5 + bcrypt |
| 对象存储 | aliyun-oss-go-sdk |
| AI 服务 | HTTP 直调 DeepSeek/OpenAI 兼容接口 |

## 后端结构

后端采用 Go 常见的 `cmd + internal + pkg` 结构，并按业务域组织代码：

- `cmd/server`
  - 程序启动入口
- `internal/bootstrap`
  - 应用装配
- `internal/database`
  - MySQL / Redis 连接工厂
- `internal/server`
  - Gin 路由和应用容器
- `internal/<domain>`
  - 按领域组织业务代码，例如 `auth`、`knowpost`、`relation`、`search`

领域包内部遵循统一职责边界：

- `handler.go`
  - HTTP 入口层，负责收参、鉴权、调用 service、写响应
- `service.go`
  - 业务编排层，负责规则、事务和跨组件协同
- `repository.go`
  - 数据访问层，负责 SQL 与持久层读写
- `model.go`
  - 领域模型与数据库映射结构
- `dto.go`
  - 请求体、响应体等传输结构

部分复杂领域会在上述基础上增加更细粒度文件，例如：

- `detail_service.go`
- `feed_service.go`
- `write_service.go`
- `outbox_consumer.go`
- `cache.go`
- `helper.go`
- `id.go`

补充约束：

- handler 依赖 `ports.go` 中定义的窄接口，而不是直接依赖具体 service
- 跨领域依赖优先在 bootstrap 构造期通过 `Deps` / `Config` 结构体注入
- 避免 `New(...)` 之后再用多个 `SetXxx(...)` 把对象补成可用状态

## 架构说明

如果你要继续维护这个项目，建议先看下面几份文档：

- [项目架构说明](docs/project-architecture.md)
  - 面向当前代码现状，解释模块边界、缓存策略、计数链路和 outbox 链路
- [后端结构重构设计](docs/superpowers/specs/2026-06-04-backend-structure-refactor-design.md)
  - 记录 bootstrap 拆分和依赖装配重构的设计背景
- [Canal / Kafka 对齐设计](docs/superpowers/specs/2026-06-05-canal-kafka-alignment-design.md)
  - 记录 outbox、Canal 和 Kafka 异步链路的演进原因

## 本地开发

### 前置条件

- Go 1.21+
- Docker Desktop 或可用的 Docker daemon
- `openssl`

### 1. 启动 Docker 服务

仓库自带 `docker-compose.yml`，当前已经更偏向单机生产部署：

- 对外只暴露前端 `80` 端口
- MySQL / Redis / Kafka / Zookeeper / Elasticsearch 都只走容器内网
- JWT 密钥通过 Docker `secrets` 注入
- 持久化数据全部使用 Docker 命名卷

```bash
make dev-up
```

会启动这些服务：

- Frontend(Nginx, `http://localhost`)
- Go API Server(`http://localhost:8080`)

- MySQL 8.0.30
- Redis 7
- Kafka(3 brokers) + Zookeeper
- Canal Server
- Elasticsearch 8.5.0

### 2. 初始化数据库

```bash
make db-init
```

### 3. 生成本地 JWT 密钥

```bash
make gen-jwt-keys
```

会生成：

- `config/keys/private.pem`
- `config/keys/public.pem`

### 4. 创建本地配置

```bash
cp config/config-local.yaml.example config/config-local.yaml
```

默认本地配置已经指向本机开发环境暴露的端口：

- MySQL: `localhost:3306`
- Redis: `localhost:6379`
- Kafka: `localhost:9092,9093,9094`
- Elasticsearch: `localhost:9200`

### 5. 运行服务

如果你只使用 Docker Compose，那么 `make dev-up` 后即可直接访问：

- 前端页面：`http://localhost`
- 前端健康检查：`http://localhost/health`
- 前端代理 API：`http://localhost/api/v1/...`

如果你希望后端继续在本机运行而不是容器里运行，也可以单独执行：

```bash
make run
```

默认等价于：

```bash
env GOCACHE=$(pwd)/.gocache go run ./cmd/server -config config/config-local.yaml
```

### 6. 常用命令

```bash
make fmt
make vet
make test
make lint
make check
make dev-logs
make dev-down
```

代码质量约定：

- `make fmt`
  - 使用 `gofmt` 统一格式化全部 Go 源码
- `make vet`
  - 运行 `go vet ./...`
- `make lint`
  - 优先运行 `golangci-lint`，如果本机未安装则回退到 `go vet`
- `make check`
  - 本地提交前的一次性质量闸门，顺序执行 `vet + lint + test`

当前这轮重构重点回归测试覆盖：

- `pkg/config`
  - 覆盖启动期默认值填充和核心配置校验
- `internal/server`
  - 覆盖应用生命周期、后台 runner 取消和 cleanup 执行
- `internal/outbox`
  - 覆盖 `partition + watermark` 幂等推进
- `internal/counter`
  - 覆盖消费端停机时内存批次 flush
- `internal/knowpost`
  - 覆盖详情缓存版本号失效策略
- `internal/search` / `internal/relation`
  - 覆盖异常 outbox 消息的兜底处理

如果本机没有 `golangci-lint`，可以安装：

```bash
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
```

### 7. Docker 构建说明

如果你使用：

```bash
docker compose build
```

当前仓库的 `Dockerfile` 已做这些优化：

- Alpine `apk` 默认走国内镜像源
- Go modules 默认走 `goproxy.cn`
- 移除了无效的 `gcc/musl-dev` 安装步骤，因为服务使用 `CGO_ENABLED=0`

前端 `frontend/Dockerfile` 也已做容器化处理：

- 构建阶段使用 `node:20-alpine`
- 运行阶段使用 `nginx:alpine`
- Nginx 会把 `/api` 代理到 Docker Compose 内部的 `app:8080`
- 浏览器访问 `http://localhost` 即可打开前端页面

当前 `docker-compose.yml` 的生产化约束：

- 后端 `app` 不再直接暴露宿主机 `8080`
- 中间件端口默认不再暴露到宿主机
- 如果需要从宿主机直接调试 MySQL/Redis/ES，需要临时加端口映射或使用 `docker compose exec`

因此：

- 第一次构建仍然会下载基础镜像和 Go 依赖，时间取决于本机网络
- 第二次及之后的构建会明显更快
- 如果再次出现长时间卡在 `apk add`，通常是 Docker Desktop 网络或镜像源连通性问题，不是 Go 编译本身的问题

## 可选能力说明

### 搜索

当 `elasticsearch.uris` 和 `elasticsearch.index_name` 配置完整时：

- `GET /api/v1/search?q=xxx`
- `GET /api/v1/search/suggest?prefix=xxx`

会启用真实搜索能力。

当 Elasticsearch 配置缺失或初始化失败时，这两个接口会返回 `503`，主服务仍可启动。

### LLM / RAG

当下列配置完整时才启用：

- `llm.deepseek.api_key`
- `llm.deepseek.base_url`
- `llm.deepseek.model`
- `llm.openai.api_key`
- `llm.openai.base_url`
- `elasticsearch.uris`

如果配置不完整：

- `POST /api/v1/knowposts/:id/description/suggest`
- `POST /api/v1/knowposts/:id/rag/query`

会返回 `503`，不会出现空指针或越界 panic。

### OSS 存储

当 `oss.endpoint / access_key_id / access_key_secret / bucket` 配置完整时才启用。

否则：

- `POST /api/v1/storage/presign`

会返回 `503`。

## API 说明

关键端点：

- `POST /api/v1/auth/register`
- `POST /api/v1/auth/login`
- `POST /api/v1/auth/refresh`
- `GET /api/v1/auth/me`
- `POST /api/v1/knowposts/draft`
- `GET /api/v1/knowposts/:id`
- `GET /api/v1/knowposts/feed/public`
- `POST /api/v1/counter/like`
- `POST /api/v1/relations/follow`
- `GET /api/v1/search?q=xxx`
- `GET /api/v1/search/suggest?prefix=xxx`

## 已修复的本地运行问题

- 需要登录的接口现在可以通过全局可选 JWT 解析拿到 `user_id`
- LLM/RAG 初始化不再依赖 `elasticsearch.uris[0]` 的裸下标访问
- 搜索、LLM、OSS 在依赖缺失时改为显式 `503`
- 搜索索引已补齐 `tag_id` mapping 兼容逻辑，旧索引无需手工删除也能支持标签过滤
- `knowpost` 搜索同步改为事务内 outbox，并由 Canal/Kafka 消费链路异步写入 Elasticsearch
- 计数器写操作现在保留 `cnt:*` SDS 快照，计数 delta 通过 `counter-events` 批量消费后直接折叠到 `cnt:*`；`like/fav` 链路新增实体级 `epoch` fence，`rebuild` 期间会短暂阻塞同实体 toggle，并让 consumer / failure worker 丢弃旧 epoch 事件与任务
- 扩展业务错误码现在会映射到正确的 HTTP 状态码
- `db/schema.sql` 的 MySQL 拼写错误已修复，可正常初始化
