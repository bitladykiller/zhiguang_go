# 接口隔离与依赖方向优化设计

## 1. 背景

经过多轮优化后，项目已具备良好的领域分包结构和依赖注入体系。但在设计层面仍存在以下可改进点：

- **DTO 跨模块耦合**：`search` 直接依赖 `knowpost.FeedItemResponse`
- **接口命名不一致**：`AuthServiceInterface` vs `CounterClient`，有的带 `Interface` 后缀，有的不带
- **共享模型缺失**：多个模块需要表示相同的 Feed 条目概念

## 2. 目标

1. 提取共享领域模型，打破模块间 DTO 耦合
2. 统一接口命名规范
3. 明确模块边界，使 `search` 不再直接依赖 `knowpost`

## 3. 设计决策

### 3.1 共享模型层

新增 `internal/model/feed_item.go`：

```go
package model

type FeedItem struct {
    ID             string
    Title          *string
    Description    *string
    CoverImage     *string
    Tags           []string
    AuthorAvatar   *string
    AuthorNickname string
    TagJson        *string
    LikeCount      int64
    FavoriteCount  int64
    Liked          *bool
    Faved          *bool
    IsTop          *bool
}
```

**设计原则**：
- 共享模型只包含纯数据字段，不含业务逻辑
- 各模块在返回 HTTP 响应时，将 FeedItem 映射到自己的 DTO 结构
- 避免 `search` 直接依赖 `knowpost` 的 DTO

### 3.2 接口命名规范

**Before**：
- `AuthServiceInterface`
- `CounterServiceInterface`
- `DescriptionServiceInterface`
- `ProfileServiceInterface`
- `RelationServiceInterface`
- `SearchServiceInterface`
- `ObjectStorage`

**After**：
- `AuthServicer`
- `CounterServicer`
- `DescriptionServicer`
- `ProfileServicer`
- `RelationServicer`
- `SearchServicer`
- `StorageServicer`

**命名规则**：
- 接口名使用 `-er` 后缀（Go 惯例，如 `Reader`、`Writer`）
- 与实现结构体区分：`AuthService`（struct）vs `AuthServicer`（interface）
- 避免使用 `Interface` 后缀，保持命名简洁

### 3.3 依赖方向

**Before**：
```
search ──import──→ knowpost (FeedItemResponse)
```

**After**：
```
search ──import──→ model (FeedItem)
knowpost ──import──→ model (FeedItem)
```

## 4. 实施结果

### 4.1 修改文件列表

| 文件 | 修改内容 |
|------|----------|
| `internal/model/feed_item.go` | 新增共享模型 |
| `internal/knowpost/dto.go` | 添加 `FeedItemFromModel` 和 `ToModel` 映射方法 |
| `internal/search/service.go` | 使用 `model.FeedItem` 替代 `knowpost.FeedItemResponse` |
| `internal/search/handler_test.go` | 更新测试引用 |
| `internal/search/service_test.go` | 更新测试引用 |
| `internal/auth/service_interface.go` | 重命名 `AuthServiceInterface` → `AuthServicer` |
| `internal/auth/handler.go` | 更新接口引用 |
| `internal/counter/service_interface.go` | 重命名 `CounterServiceInterface` → `CounterServicer` |
| `internal/counter/handler.go` | 更新接口引用 |
| `internal/llm/service_interface.go` | 重命名 `DescriptionServiceInterface` → `DescriptionServicer` |
| `internal/llm/handler.go` | 更新接口引用 |
| `internal/profile/service_interface.go` | 重命名 `ProfileServiceInterface` → `ProfileServicer` |
| `internal/profile/handler.go` | 更新接口引用 |
| `internal/relation/service_interface.go` | 重命名 `RelationServiceInterface` → `RelationServicer` |
| `internal/relation/handler.go` | 更新接口引用 |
| `internal/search/service_interface.go` | 重命名 `SearchServiceInterface` → `SearchServicer` |
| `internal/search/handler.go` | 更新接口引用 |
| `internal/storage/service_interface.go` | 重命名 `ObjectStorage` → `StorageServicer` |
| `internal/storage/handler.go` | 更新接口引用 |
| `internal/bootstrap/init_auth.go` | 更新接口引用 |

### 4.2 验证结果

- `go build ./...` ✅
- `go test ./...` ✅
- `go vet ./...` ✅

## 5. 收益

1. **模块边界清晰**：`search` 不再依赖 `knowpost`，依赖关系更合理
2. **命名一致**：所有接口统一使用 `-er` 后缀，符合 Go 惯例
3. **可测试性提升**：共享模型可被多个模块独立测试
4. **可维护性**：新增模块时可以直接复用 `model.FeedItem`，无需重复定义

## 6. 后续建议

- 考虑将 `KnowPostDetailResponse` 等更多 DTO 提取到共享模型
- 评估 `internal/model` 是否需要进一步拆分为 `model/feed`、`model/user` 等子包
- 考虑为共享模型添加 `Validate()` 等方法，统一数据校验逻辑
