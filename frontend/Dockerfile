# Stage 1 — 构建前端静态资源
FROM node:20-alpine AS builder

ARG ALPINE_REPO=https://mirrors.aliyun.com/alpine
ARG NPM_REGISTRY=https://registry.npmmirror.com

# --- [基础环境] --- //
# 使用国内 Alpine/NPM 源，降低首次构建在国内网络环境下的失败率和耗时。
RUN sed -i "s#https\\?://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO}#g" /etc/apk/repositories && \
    npm config set registry "${NPM_REGISTRY}"

WORKDIR /app

# --- [依赖安装] --- //
# 先复制依赖描述文件，尽量复用 Docker 层缓存，避免源码一变就重新安装全部依赖。
COPY package.json package-lock.json ./
RUN npm ci

COPY . .

# --- [前端构建] --- //
# 生产环境默认走相对路径访问 `/api`，由运行期 Nginx 统一反向代理到后端服务。
RUN npm run build

# Stage 2 — 使用 Nginx 提供静态资源与 API 反向代理
FROM nginx:1.27-alpine

ARG ALPINE_REPO=https://mirrors.aliyun.com/alpine

RUN sed -i "s#https\\?://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO}#g" /etc/apk/repositories

WORKDIR /usr/share/nginx/html

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /app/dist ./

EXPOSE 80

USER nginx
