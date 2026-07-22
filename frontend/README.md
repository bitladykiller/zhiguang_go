# 知光前端

知光前端是基于 Vite、React、TypeScript 重建的知识社区界面，重点目标是：

- 精美：使用统一设计 token、克制阴影、深墨蓝侧栏、本地封面资产和清晰内容层级。
- 工程化：按 `app / layouts / components / features / services / pages / styles / types` 分层。
- 可运行：后端不可用时，内容服务会回退到本地 mock 数据，便于独立开发和视觉验证。
- 可接入：开发服务继续代理 `/api` 到 `http://localhost:8080`，生产 Nginx 代理到 Docker Compose 内的 `app:8080`。

## 本地开发

```bash
npm install
npm run dev
```

默认访问：

```text
http://127.0.0.1:5173/
```

如果端口被占用，可以手动指定：

```bash
npm run dev -- --host 127.0.0.1 --port 5174
```

## 验证

```bash
npm run build
npm run lint
```

当前 `build` 会先运行 TypeScript 类型检查，再执行 Vite 生产构建。

## 目录结构

```text
src/
  app/          路由和应用入口
  components/   通用 UI 与内容卡片
  features/     领域状态，例如 auth
  layouts/      应用主布局
  pages/        页面级组合
  services/     API 适配与 mock 回退
  styles/       全局样式与设计 token
  types/        领域类型
```
