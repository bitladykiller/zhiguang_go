// ZhiGuang（知光）Go Server —— 知识获取与分享社区的后端服务。
//
// 本文件是程序的启动入口，负责解析命令行参数并委托 bootstrap 包
// 完成全部依赖注入与应用装配，然后启动 HTTP 服务并阻塞等待退出。
//
// 设计决策：
//   - 采用手动依赖注入，不依赖 wire 等代码生成工具。
//     这样做虽然增加了启动代码量，但所有依赖关系对开发者可见、可调试。
//   - 装配逻辑集中在 bootstrap.InitializeApp，便于测试时替换完整依赖图。
//
// 使用方式：
//   go run ./cmd/server -config config/config-local.yaml
package main

import (
	"flag"
	"log"

	"github.com/zhiguang/app/internal/bootstrap"
	"github.com/zhiguang/app/internal/server"
)

func main() {
	// 解析命令行参数：--config 指定配置文件路径，默认指向 config/config.yaml
	configPath := flag.String("config", "config/config.yaml", "path to configuration file")
	flag.Parse()

	// 初始化应用：该调用会完成所有数据库/缓存/消息队列连接的建立，
	// 以及所有业务服务的装配和路由注册。
	app, err := initializeApp(*configPath)
	if err != nil {
		log.Fatalf("Failed to initialize application: %v", err)
	}

	// 启动 HTTP 服务并阻塞等待退出信号
	if err := app.Run(); err != nil {
		log.Fatalf("Application error: %v", err)
	}
}

// initializeApp 通过手动装配的方式完成全部依赖注入，不依赖代码生成。
//
// WHY：使用函数而非结构体方法是为了让 main 函数保持清晰——它只做两件事：
// 解析配置和启动服务。装配细节完全隐藏在 bootstrap 包中。
func initializeApp(configPath string) (*server.App, error) {
	return bootstrap.InitializeApp(configPath)
}
