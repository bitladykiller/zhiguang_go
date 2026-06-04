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
- `canal.enabled=true` 时，会切换为与 Java 版一致的 `Canal -> Kafka -> relation/search consumers` 链路
- `canal.enabled=false` 时，不会启动异步 outbox 消费链路
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

## 本地开发

### 前置条件

- Go 1.21+
- Docker Desktop 或可用的 Docker daemon
- `openssl`

### 1. 启动依赖服务

仓库自带 `docker-compose.yml`，所有持久化都使用 Docker 命名卷，不使用宿主机 bind mount。

```bash
make dev-up
```

会启动这些依赖：

- MySQL 8.0.30
- Redis 7
- Kafka + Zookeeper
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

默认本地配置已经指向 Docker Compose 暴露的端口：

- MySQL: `localhost:3306`
- Redis: `localhost:6379`
- Kafka: `localhost:9092`
- Elasticsearch: `localhost:9200`

### 5. 运行服务

```bash
make run
```

默认等价于：

```bash
env GOCACHE=$(pwd)/.gocache go run ./cmd/server -config config/config-local.yaml
```

### 6. 常用命令

```bash
make test
make lint
make dev-logs
make dev-down
```

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
- 计数器写操作现在会主动失效 SDS，读取时可从位图重建，避免 Kafka 异步链路故障时长期返回错误计数
- 扩展业务错误码现在会映射到正确的 HTTP 状态码
- `db/schema.sql` 的 MySQL 拼写错误已修复，可正常初始化
