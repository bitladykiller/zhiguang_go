package bootstrap

import (
	"github.com/jmoiron/sqlx"

	"github.com/zhiguang/app/internal/profile"
)

// initProfile 创建资料模块的完整服务栈。
//
// 创建顺序：
//   1. ProfileRepository（MySQL users 表 CRUD）
//   2. ProfileService（资料查询 + 编辑 + 权限校验）
//   3. ProfileHandler（HTTP 请求适配）
//
// 返回：
//   - *profile.ProfileHandler: HTTP handler
func initProfile(db *sqlx.DB) *profile.ProfileHandler {
	profileRepo := profile.NewProfileRepository(db)
	profileSvc := profile.NewProfileService(profileRepo)
	return profile.NewProfileHandler(profileSvc)
}
