# 知光（ZhiGuang）API 文档

> 知识获取与分享社区后端服务 API 文档
>
> Base URL: `http://localhost:8080/api/v1`（本地开发环境）

---

## 目录

- [概述](#概述)
- [通用说明](#通用说明)
  - [认证方式](#认证方式)
  - [响应格式](#响应格式)
  - [错误码](#错误码)
- [鉴权模块 (Auth)](#鉴权模块-auth)
- [知文模块 (KnowPost)](#知文模块-knowpost)
- [计数模块 (Counter)](#计数模块-counter)
- [关系模块 (Relation)](#关系模块-relation)
- [搜索模块 (Search)](#搜索模块-search)
- [存储模块 (Storage)](#存储模块-storage)
- [资料模块 (Profile)](#资料模块-profile)
- [AI 模块 (LLM)](#ai模块-llm)
- [健康检查](#健康检查)

---

## 概述

知光是一个知识获取与分享社区的后端服务，提供用户认证、知文发布与浏览、点赞收藏、关注关系、全文搜索、对象存储、用户资料和 AI 摘要等功能。

### 技术栈

- **框架**: Gin
- **数据库**: MySQL
- **缓存**: Redis（支持本地缓存 + Redis 二级缓存）
- **消息队列**: Kafka
- **搜索**: Elasticsearch
- **对象存储**: 阿里云 OSS
- **AI 服务**: DeepSeek API

---

## 通用说明

### 认证方式

本服务使用 JWT (JSON Web Token) 进行身份认证。

**请求头格式**:
```
Authorization: Bearer <access_token>
```

**令牌类型**:
- `access_token`: 短期令牌（默认 15 分钟有效期）
- `refresh_token`: 长期令牌（默认 7 天有效期），用于刷新 access_token

**认证级别**:
- **公开接口**: 无需认证
- **可选认证**: 支持匿名访问，登录用户获得额外信息（如点赞状态）
- **需要认证**: 必须携带有效 access_token

### 响应格式

所有接口统一返回 JSON 格式：

**成功响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": { ... }
}
```

**创建成功响应**:
```json
{
  "code": 201,
  "message": "created",
  "data": { ... }
}
```

**错误响应**:
```json
{
  "code": 40001,
  "message": "错误描述",
  "data": null
}
```

### 错误码

| HTTP 状态码 | 业务错误码 | 说明 |
|------------|-----------|------|
| 200 | - | 成功 |
| 201 | - | 创建成功 |
| 400 | 40001 | 请求参数错误 |
| 401 | 40101 | 未授权（未登录或令牌无效） |
| 403 | 40301 | 禁止访问（无权限） |
| 404 | 40401 | 资源不存在 |
| 429 | 42901 | 请求过于频繁 |
| 500 | 50001 | 服务器内部错误 |
| 503 | 50301 | 服务不可用（可选功能未配置） |

---

## 鉴权模块 (Auth)

基础路径: `/api/v1/auth`

### 发送验证码

**POST** `/auth/send-code`

发送验证码到手机或邮箱。

**请求体**:
```json
{
  "identifier": "13800138000",
  "identifier_type": "PHONE",
  "scene": "REGISTER"
}
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| identifier | string | 是 | 用户标识（手机号或邮箱） |
| identifier_type | string | 是 | 标识类型：`PHONE` / `EMAIL` |
| scene | string | 是 | 场景：`REGISTER` / `LOGIN` / `RESET_PASSWORD` |

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "identifier": "13800138000",
    "scene": "REGISTER",
    "expire_seconds": 300
  }
}
```

**认证**: 无需认证

---

### 用户注册

**POST** `/auth/register`

使用验证码注册新用户。

**请求体**:
```json
{
  "identifier": "13800138000",
  "identifier_type": "PHONE",
  "code": "123456",
  "password": "your_password",
  "agree_terms": true
}
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| identifier | string | 是 | 用户标识 |
| identifier_type | string | 是 | 标识类型 |
| code | string | 是 | 验证码 |
| password | string | 否 | 密码（可选，后续可用验证码登录） |
| agree_terms | boolean | 是 | 必须为 true |

**响应**:
```json
{
  "code": 201,
  "message": "created",
  "data": {
    "user": {
      "id": 123456789,
      "nickname": "用户昵称",
      "avatar": "https://...",
      "phone": "138****8000"
    },
    "token": {
      "access_token": "eyJhbGciOiJIUzI1NiIs...",
      "access_token_expires_at": "2024-01-01T12:15:00Z",
      "refresh_token": "eyJhbGciOiJIUzI1NiIs...",
      "refresh_token_expires_at": "2024-01-08T12:00:00Z"
    }
  }
}
```

**认证**: 无需认证

---

### 用户登录

**POST** `/auth/login`

支持密码登录和验证码登录两种方式。

**请求体**:
```json
{
  "identifier": "13800138000",
  "identifier_type": "PHONE",
  "password": "your_password"
}
```

或验证码登录：
```json
{
  "identifier": "13800138000",
  "identifier_type": "PHONE",
  "code": "123456"
}
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| identifier | string | 是 | 用户标识 |
| identifier_type | string | 是 | 标识类型 |
| password | string | 否* | 密码（与 code 二选一） |
| code | string | 否* | 验证码（与 password 二选一） |

**响应**: 同注册接口

**认证**: 无需认证

---

### 刷新令牌

**POST** `/auth/refresh`

使用 refresh_token 获取新的令牌对。

**请求体**:
```json
{
  "refresh_token": "eyJhbGciOiJIUzI1NiIs..."
}
```

**响应**: 同登录接口（返回新令牌对）

**认证**: 无需认证

> **令牌轮换**: 每次刷新会使旧的 refresh_token 失效，返回新的令牌对。

---

### 用户登出

**POST** `/auth/logout`

登出当前用户，吊销 refresh_token。

**请求体**:
```json
{
  "refresh_token": "eyJhbGciOiJIUzI1NiIs..."
}
```

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "message": "logged out"
  }
}
```

**认证**: 无需认证

> **注意**: access_token 在过期前仍有效（JWT 无状态特性），建议客户端登出后立即清除本地令牌。

---

### 重置密码

**POST** `/auth/reset-password`

通过验证码重置密码，会吊销该用户所有设备的 refresh_token。

**请求体**:
```json
{
  "identifier": "13800138000",
  "identifier_type": "PHONE",
  "code": "123456",
  "new_password": "new_password"
}
```

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "message": "password reset successful"
  }
}
```

**认证**: 无需认证

---

### 获取当前用户信息

**GET** `/auth/me`

获取当前登录用户的详细信息。

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "id": 123456789,
    "nickname": "用户昵称",
    "avatar": "https://...",
    "phone": "138****8000",
    "zg_id": "zg_12345",
    "birthday": "1990-01-01T00:00:00Z",
    "school": "某某大学",
    "bio": "个人简介",
    "gender": "male",
    "tags_json": "[\"标签1\", \"标签2\"]"
  }
}
```

**认证**: 需要认证

---

## 知文模块 (KnowPost)

基础路径: `/api/v1/knowposts`

### 创建草稿

**POST** `/knowposts/draft`

创建一篇新的知文草稿。

**请求体**: 无

**响应**:
```json
{
  "code": 201,
  "message": "created",
  "data": {
    "id": "1234567890123456789"
  }
}
```

**认证**: 需要认证

---

### 确认内容

**PUT** `/knowposts/:id/content`

客户端完成 OSS 直传后，通知服务端确认内容。

**路径参数**:
- `id`: 知文 ID

**请求体**:
```json
{
  "object_key": "content/uuid_filename.pdf",
  "etag": "\"abc123...\"",
  "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "size": 12345
}
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| object_key | string | 是 | OSS 对象键 |
| etag | string | 是 | OSS ETag |
| sha256 | string | 是 | 文件 SHA256 哈希 |
| size | uint64 | 是 | 文件大小（字节） |

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "success": true
  }
}
```

**认证**: 需要认证

---

### 更新元数据

**PUT** `/knowposts/:id/metadata`

部分更新知文的元数据（标题、标签、描述等）。

**路径参数**:
- `id`: 知文 ID

**请求体**:
```json
{
  "title": "知文标题",
  "description": "知文简介",
  "tags": ["标签1", "标签2"],
  "img_urls": ["https://.../cover.jpg"],
  "visible": "public",
  "is_top": false
}
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| title | string | 否 | 标题 |
| description | string | 否 | 简介 |
| tags | []string | 否 | 标签列表 |
| img_urls | []string | 否 | 图片 URL 列表 |
| visible | string | 否 | 可见性：`public` / `private` / `followers` |
| is_top | boolean | 否 | 是否置顶 |

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "success": true
  }
}
```

**认证**: 需要认证

---

### 发布知文

**POST** `/knowposts/:id/publish`

将草稿状态的知文发布。

**路径参数**:
- `id`: 知文 ID

**请求体**: 无

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "success": true
  }
}
```

**认证**: 需要认证

---

### 更新置顶状态

**PUT** `/knowposts/:id/top`

切换知文的置顶状态。

**路径参数**:
- `id`: 知文 ID

**请求体**:
```json
{
  "is_top": true
}
```

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "success": true
  }
}
```

**认证**: 需要认证

---

### 更新可见性

**PUT** `/knowposts/:id/visibility`

更新知文的可见性设置。

**路径参数**:
- `id`: 知文 ID

**请求体**:
```json
{
  "visible": "public"
}
```

| visible 值 | 说明 |
|-----------|------|
| public | 公开可见 |
| private | 仅自己可见 |
| followers | 仅粉丝可见 |

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "success": true
  }
}
```

**认证**: 需要认证

---

### 删除知文

**DELETE** `/knowposts/:id`

软删除知文。

**路径参数**:
- `id`: 知文 ID

**请求体**: 无

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "success": true
  }
}
```

**认证**: 需要认证

---

### 获取知文详情

**GET** `/knowposts/:id`

获取知文的详细信息。

**路径参数**:
- `id`: 知文 ID

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "id": "1234567890123456789",
    "title": "知文标题",
    "description": "知文简介",
    "content_url": "https://.../content.pdf",
    "images": ["https://.../1.jpg", "https://.../2.jpg"],
    "tags": ["标签1", "标签2"],
    "author_id": "123456789",
    "author_avatar": "https://.../avatar.jpg",
    "author_nickname": "作者昵称",
    "author_tag_json": "[\"标签\"]",
    "like_count": 42,
    "favorite_count": 10,
    "liked": true,
    "faved": false,
    "is_top": false,
    "visible": "public",
    "type": "pdf",
    "publish_time": "2024-01-01T10:00:00Z"
  }
}
```

| 字段 | 说明 |
|------|------|
| liked | 当前用户是否已点赞（登录用户专属字段） |
| faved | 当前用户是否已收藏（登录用户专属字段） |

**认证**: 可选认证（登录用户获得点赞/收藏状态）

---

### 获取公共 Feed

**GET** `/knowposts/feed/public`

获取公开的知文列表，支持分页。

**查询参数**:
| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| page | int | 否 | 1 | 页码 |
| size | int | 否 | 20 | 每页数量 |

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "items": [
      {
        "id": "1234567890123456789",
        "title": "知文标题",
        "description": "简介",
        "cover_image": "https://.../cover.jpg",
        "tags": ["标签1", "标签2"],
        "author_avatar": "https://.../avatar.jpg",
        "author_nickname": "作者昵称",
        "tag_json": "[\"标签\"]",
        "like_count": 42,
        "favorite_count": 10,
        "liked": true,
        "faved": false
      }
    ],
    "page": 1,
    "size": 20,
    "has_more": true
  }
}
```

**认证**: 可选认证

---

### 获取我的已发布

**GET** `/knowposts/feed/mine`

获取当前用户已发布的知文列表。

**查询参数**:
| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| page | int | 否 | 1 | 页码 |
| size | int | 否 | 20 | 每页数量 |

**响应**: 同公共 Feed

**认证**: 需要认证

---

## 计数模块 (Counter)

基础路径: `/api/v1/counter`

### 点赞

**POST** `/counter/like`

为指定实体点赞。

**请求体**:
```json
{
  "entity_type": "knowpost",
  "entity_id": "1234567890123456789"
}
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| entity_type | string | 是 | 实体类型（如 `knowpost`） |
| entity_id | string | 是 | 实体 ID |

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "success": true,
    "changed": true
  }
}
```

| 字段 | 说明 |
|------|------|
| changed | `true` 表示状态发生变化，`false` 表示重复点赞 |

**认证**: 需要认证

---

### 取消点赞

**POST** `/counter/unlike`

取消点赞。

**请求体**: 同点赞

**响应**: 同点赞

**认证**: 需要认证

---

### 收藏

**POST** `/counter/fav`

收藏指定实体。

**请求体**: 同点赞

**响应**: 同点赞

**认证**: 需要认证

---

### 取消收藏

**POST** `/counter/unfav`

取消收藏。

**请求体**: 同点赞

**响应**: 同点赞

**认证**: 需要认证

---

### 获取计数

**GET** `/counter/counts`

获取指定实体的点赞数、收藏数等计数。

**查询参数**:
| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| entity_type | string | 是 | - | 实体类型 |
| entity_id | string | 是 | - | 实体 ID |
| metrics | string | 否 | `like,fav` | 指标列表，逗号分隔 |

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "data": {
      "like": 42,
      "fav": 10
    }
  }
}
```

**认证**: 无需认证

---

### 获取用户状态

**GET** `/counter/status`

获取当前用户对指定实体的点赞/收藏状态。

**查询参数**:
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| entity_type | string | 是 | 实体类型 |
| entity_id | string | 是 | 实体 ID |

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "is_liked": true,
    "is_faved": false
  }
}
```

**认证**: 需要认证

---

## 关系模块 (Relation)

基础路径: `/api/v1/relations`

### 关注用户

**POST** `/relations/follow`

关注指定用户。

**请求体**:
```json
{
  "to_user_id": 123456789
}
```

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "success": true
  }
}
```

**错误情况**:
- 关注自己：400 "cannot follow yourself"
- 已关注/限流：429 "rate limited or already following"

**认证**: 需要认证

---

### 取消关注

**POST** `/relations/unfollow`

取消关注指定用户。

**请求体**:
```json
{
  "to_user_id": 123456789
}
```

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "success": true,
    "changed": true
  }
}
```

| 字段 | 说明 |
|------|------|
| changed | `true` 表示取关成功，`false` 表示之前就未关注 |

**认证**: 需要认证

---

### 获取关系状态

**GET** `/relations/status`

获取当前用户与目标用户的关系状态。

**查询参数**:
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| other_id | uint64 | 是 | 目标用户 ID |

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "status": "mutual"
  }
}
```

| status 值 | 说明 |
|-----------|------|
| mutual | 互关 |
| following | 当前用户关注了对方 |
| followed | 对方关注了当前用户 |
| none | 无关系 |

**认证**: 需要认证

---

### 获取关注列表（Offset 分页）

**GET** `/relations/following`

获取某用户关注的人列表。

**查询参数**:
| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| user_id | uint64 | 是 | - | 用户 ID |
| limit | int | 否 | 20 | 每页数量 |
| offset | int | 否 | 0 | 偏移量 |

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "data": [
      {
        "user_id": 123456789,
        "nickname": "用户昵称",
        "avatar": "https://..."
      }
    ]
  }
}
```

**认证**: 无需认证

---

### 获取粉丝列表（Offset 分页）

**GET** `/relations/followers`

获取某用户的粉丝列表。

**查询参数**: 同关注列表

**响应**: 同关注列表

**认证**: 无需认证

---

### 获取关注列表（游标分页）

**GET** `/relations/following/cursor`

使用游标分页获取关注列表，适合无限滚动场景。

**查询参数**:
| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| user_id | uint64 | 是 | - | 用户 ID |
| limit | int | 否 | 20 | 每页数量 |
| cursor | int64 | 否 | 0 | 游标（时间戳），0 表示从头开始 |

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "data": [...],
    "cursor": 1704067200000,
    "has_more": true
  }
}
```

**认证**: 无需认证

---

### 获取粉丝列表（游标分页）

**GET** `/relations/followers/cursor`

使用游标分页获取粉丝列表。

**查询参数**: 同关注列表游标分页

**响应**: 同关注列表游标分页

**认证**: 无需认证

---

## 搜索模块 (Search)

基础路径: `/api/v1/search`

> **注意**: 搜索功能依赖 Elasticsearch，如果 ES 未配置或不可用，接口返回 503。

### 全文搜索

**GET** `/search`

搜索知文内容。

**查询参数**:
| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| q | string | 是 | - | 搜索关键词 |
| size | int | 否 | 20 | 每页数量 |
| tags | string | 否 | - | 标签筛选，逗号分隔 |
| after | string | 否 | - | 游标值（上一页返回的 next_after） |

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "items": [
      {
        "id": "1234567890123456789",
        "title": "知文标题",
        "description": "简介",
        "cover_image": "https://...",
        "tags": ["标签1"],
        "author_nickname": "作者",
        "like_count": 42,
        "favorite_count": 10,
        "liked": true,
        "faved": false
      }
    ],
    "next_after": "eyJpZCI6IjEyMzQ1NiJ9",
    "has_more": true
  }
}
```

**认证**: 可选认证（登录用户获得点赞/收藏状态）

---

### 搜索建议

**GET** `/search/suggest`

获取搜索关键词的自动补全建议。

**查询参数**:
| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| prefix | string | 是 | - | 前缀关键词 |
| size | int | 否 | 10 | 建议数量 |

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "items": ["Go语言入门", "Go并发编程", "Go微服务"]
  }
}
```

**认证**: 无需认证

---

## 存储模块 (Storage)

基础路径: `/api/v1/storage`

> **注意**: 存储功能依赖阿里云 OSS，如果 OSS 未配置，接口返回 503。

### 获取预签名上传 URL

**POST** `/storage/presign`

获取 OSS 直传的预签名 URL，支持客户端直接上传文件到 OSS。

**请求体**:
```json
{
  "file_name": "document.pdf",
  "content_type": "application/pdf",
  "folder": "content"
}
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| file_name | string | 是 | 文件名 |
| content_type | string | 是 | MIME 类型 |
| folder | string | 否 | 存储文件夹，默认根目录 |

**响应**:
```json
{
  "code": 201,
  "message": "created",
  "data": {
    "upload_url": "https://bucket.oss-cn-hangzhou.aliyuncs.com/content/uuid_document.pdf?OSSAccessKeyId=...&Signature=...",
    "object_key": "content/uuid_document.pdf",
    "public_url": "https://cdn.example.com/content/uuid_document.pdf",
    "expire_at": "2024-01-01T12:10:00Z"
  }
}
```

**使用流程**:
1. 客户端调用此接口获取 `upload_url`
2. 客户端直接向 `upload_url` 发送 `PUT` 请求上传文件
3. 上传成功后，调用知文的 `确认内容` 接口传递 `object_key`

**认证**: 需要认证

---

## 资料模块 (Profile)

基础路径: `/api/v1/profiles`

### 获取用户资料

**GET** `/profiles/:id`

获取指定用户的公开资料。

**路径参数**:
- `id`: 用户 ID

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "id": 123456789,
    "nickname": "用户昵称",
    "avatar": "https://.../avatar.jpg",
    "phone": "138****8000",
    "zg_id": "zg_12345",
    "birthday": "1990-01-01T00:00:00Z",
    "school": "某某大学",
    "bio": "个人简介",
    "gender": "male",
    "tags_json": "[\"标签1\", \"标签2\"]"
  }
}
```

**认证**: 无需认证

---

### 更新用户资料

**PATCH** `/profiles/:id`

更新当前用户的资料（仅限本人）。

**路径参数**:
- `id`: 用户 ID（必须与当前登录用户一致）

**请求体**:
```json
{
  "nickname": "新昵称",
  "avatar": "https://.../new_avatar.jpg",
  "bio": "新的个人简介",
  "gender": "male",
  "birthday": "1990-01-01T00:00:00Z",
  "school": "新学校",
  "tags_json": "[\"新标签\"]"
}
```

所有字段均为可选，只更新请求中包含的字段。

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "success": true
  }
}
```

**错误情况**:
- 未登录：401
- 修改他人资料：403
- 无更新字段：400

**认证**: 需要认证

---

## AI 模块 (LLM)

基础路径: `/api/v1/knowposts`

> **注意**: AI 功能依赖 DeepSeek API，如果未配置，接口返回 503。

### AI 摘要生成

**POST** `/knowposts/:id/description/suggest`

使用 AI 为知文生成摘要。

**路径参数**:
- `id`: 知文 ID

**请求体**:
```json
{
  "title": "知文标题",
  "content": "知文内容..."
}
```

**响应**:
```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "description": "这是 AI 生成的摘要内容..."
  }
}
```

**认证**: 需要认证

---

### RAG 问答

**POST** `/knowposts/:id/rag/query`

基于知文内容的 RAG（检索增强生成）问答，使用 SSE 流式返回。

**路径参数**:
- `id`: 知文 ID

**请求体**:
```json
{
  "question": "这篇文章的核心观点是什么？"
}
```

**响应**:
- Content-Type: `text/event-stream`
- Cache-Control: `no-cache`
- Connection: `keep-alive`

**SSE 数据流示例**:
```
这是
一篇
关于
...
```

**认证**: 需要认证

---

## 健康检查

### 服务健康状态

**GET** `/health`

检查服务是否存活。

**响应**:
```json
{
  "status": "ok"
}
```

**认证**: 无需认证

---

## 附录

### 枚举值

#### IdentifierType（标识类型）
| 值 | 说明 |
|----|------|
| PHONE | 手机号 |
| EMAIL | 邮箱 |

#### VerificationScene（验证码场景）
| 值 | 说明 |
|----|------|
| REGISTER | 注册 |
| LOGIN | 登录 |
| RESET_PASSWORD | 重置密码 |

#### Visible（可见性）
| 值 | 说明 |
|----|------|
| public | 公开 |
| private | 私有（仅自己） |
| followers | 仅粉丝可见 |

#### RelationStatus（关系状态）
| 值 | 说明 |
|----|------|
| mutual | 互相关注 |
| following | 已关注 |
| followed | 被关注 |
| none | 无关系 |

---

### 请求限流

以下接口有请求限流保护：

| 接口 | 限流策略 |
|------|---------|
| POST /auth/send-code | 每手机号/邮箱每日上限 |
| POST /relations/follow | 操作频率限制 |

---

### 版本信息

- API 版本: v1
- 基础路径: `/api/v1`
- 更新日期: 2024-01-01

---

> 本文档由代码注释自动生成，如有疑问请参考 `internal/*/handler*.go` 源码。
