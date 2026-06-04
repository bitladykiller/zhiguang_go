// ZhiGuang（知光）Go Server：知识获取与分享社区的后端服务。
//
// 当前使用手动依赖注入，装配入口位于 internal/bootstrap。
package main

import (
	"flag"
	"log"

	"github.com/zhiguang/app/internal/bootstrap"
	"github.com/zhiguang/app/internal/server"
)

func main() {
	configPath := flag.String("config", "config/config.yaml", "path to configuration file")
	flag.Parse()

	app, err := initializeApp(*configPath)
	if err != nil {
		log.Fatalf("Failed to initialize application: %v", err)
	}

	if err := app.Run(); err != nil {
		log.Fatalf("Application error: %v", err)
	}
}

// initializeApp 通过手动装配的方式完成全部依赖注入，不依赖代码生成。
func initializeApp(configPath string) (*server.App, error) {
	return bootstrap.InitializeApp(configPath)
}
