# 知光前端

知光前端是基于 Vite、React、TypeScript 构建的知识社区界面，视觉主题为**「夜观星象 · 鎏金书院」**：

- 概念：知光，即长夜里的知识之光。整站以深夜墨蓝星空为底，鎏金为知识之光，朱砂印章为落款，衬线字体（Didot 拉丁 × 宋体中文）承担展示层。
- 装饰体系：星场与颗粒噪点背景、星座连线（`Constellation`）、朱砂印章（`SealMark`）、鎏金分隔线（`Ornament`）、竖排刊头、鎏金流光文字（`.gilded`）、错峰浮现动画（`.rise` / `.bloom` / `.d1`–`.d8`）。
- 工程化：按 `app / layouts / components / features / services / pages / styles / types` 分层；所有颜色、字体、阴影、圆角集中在 `src/styles/tokens.css` 设计 token 中。
- 可运行：后端不可用时，内容服务会回退到本地 mock 数据，便于独立开发和视觉验证。
- 可接入：开发服务继续代理 `/api` 到 `http://localhost:8080`，生产 Nginx 代理到 Docker Compose 内的 `app:8080`。
- 可访问性：全局 `prefers-reduced-motion` 兜底、`:focus-visible` 鎏金焦点环、语义化导航与表单标签。

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
  app/               路由和应用入口
  components/        通用 UI 与内容卡片
    decor/           装饰组件：SealMark 印章 / Ornament 分隔线 / Constellation 星座
    ui/              基础组件：Button / Tag / EmptyState
  features/          领域状态，例如 auth
  layouts/           应用主布局（观星台侧栏 + 毛玻璃顶栏 + 移动端底栏）
  pages/             页面级组合与页面级样式模块
  services/          API 适配与 mock 回退
  styles/            全局样式（星场背景、关键帧、动效工具类）与设计 token
  types/             领域类型
```

## 设计 token 速览

| 类别 | 变量 | 说明 |
| --- | --- | --- |
| 夜空底色 | `--night-0` ~ `--night-4` | 由深至浅的五层墨蓝 |
| 月光文字 | `--moon-100` ~ `--moon-600` | 带宣纸暖意的文字色阶 |
| 鎏金 | `--gold-200` ~ `--gold-600`、`--gold-sheen` | 强调色与流光渐变 |
| 印章与辅助 | `--seal`、`--lapis`、`--jade` | 朱砂 / 青金 / 玉 |
| 字体 | `--font-display / body / mono` | 衬线展示、无衬线正文、等宽数字标签 |

修改主题时优先调整 token，避免在组件内写死颜色。
