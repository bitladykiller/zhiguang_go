# Stage 1 — 编译
FROM golang:1.25-alpine AS builder

ARG ALPINE_REPO=https://mirrors.aliyun.com/alpine
ARG GOPROXY_URL=https://goproxy.cn,direct

# --- [基础环境] --- //
# 使用国内 Alpine 源和 Go 代理，避免默认国外源导致的长时间卡顿。
# 当前服务使用 CGO_ENABLED=0 静态编译，不需要 gcc/musl-dev 这类 C 工具链。
RUN sed -i "s#https\\?://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO}#g" /etc/apk/repositories

ENV GOPROXY=${GOPROXY_URL} \
    GOSUMDB=sum.golang.google.cn \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

WORKDIR /build

# --- [依赖缓存] --- //
# 先复制依赖清单，让 Docker 层缓存尽量命中，避免源码一变就重新下载全部依赖。
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# --- [应用编译] --- //
RUN go build -ldflags="-s -w" -o /build/server ./cmd/server

# Stage 2 — 运行
FROM alpine:3.21

ARG ALPINE_REPO=https://mirrors.aliyun.com/alpine

RUN sed -i "s#https\\?://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO}#g" /etc/apk/repositories && \
    apk add --no-cache ca-certificates tzdata && \
    cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone

WORKDIR /app

COPY --from=builder /build/server .
COPY config/ ./config/

EXPOSE 8080

USER nobody

ENTRYPOINT ["./server"]
CMD ["-config", "config/config.yaml"]
