#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV="${1:-docker}"

case "$ENV" in
  docker)
    COMPOSE_ARGS=""
    ;;
  local)
    COMPOSE_ARGS="-f docker-compose.yml -f docker-compose.local.yml"
    ;;
  *)
    echo "Usage: $0 [docker|local]"
    exit 1
    ;;
esac

echo "==> 构建应用镜像..."
docker compose $COMPOSE_ARGS build app frontend

echo "==> 启动全部服务..."
docker compose $COMPOSE_ARGS up -d

echo "==> 等待 MySQL 就绪..."
until docker compose $COMPOSE_ARGS exec -T mysql mysqladmin ping -h localhost -proot123 --silent 2>/dev/null; do
  echo "   MySQL 未就绪，等 3 秒..."
  sleep 3
done

echo "==> 等待 App 健康检查..."
for i in $(seq 1 30); do
  if curl -s http://localhost/health > /dev/null 2>&1; then
    echo "   App 已就绪 ✓"
    break
  fi
  sleep 2
done

echo ""
echo "  部署完成！"
echo "  前端地址: http://localhost"
echo "  API 健康检查: http://localhost/health"
echo "  查看日志: docker compose $COMPOSE_ARGS logs -f app"
echo "  停止服务: docker compose $COMPOSE_ARGS down"
