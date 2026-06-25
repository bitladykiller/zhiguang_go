package sqlutil

import "strings"

// BuildSetClause 构建 SQL UPDATE 的 SET 子句和参数。
//
// 调用示例:
//
//	sets, args := sqlutil.BuildSetClause(
//	    sqlutil.Set("update_time = ?", time.Now()),
//	    sqlutil.SetIf(nickname != nil, "nickname = ?", *nickname),
//	    sqlutil.SetIf(avatar != nil, "avatar = ?", *avatar),
//	)
//	args = append(args, id)
//	query := "UPDATE users SET " + sets + " WHERE id = ?"
func BuildSetClause(entries ...SetEntry) (string, []interface{}) {
	sets := make([]string, 0, len(entries))
	args := make([]interface{}, 0, len(entries))
	for _, e := range entries {
		if !e.include {
			continue
		}
		sets = append(sets, e.clause)
		args = append(args, e.value)
	}
	return strings.Join(sets, ", "), args
}

// SetEntry 表示一个 SET 子句条目。
type SetEntry struct {
	include bool
	clause  string
	value   interface{}
}

// Set 创建一个始终包含的 SET 条目。
func Set(clause string, value interface{}) SetEntry {
	return SetEntry{include: true, clause: clause, value: value}
}

// SetIf 创建一个条件包含的 SET 条目。
func SetIf(condition bool, clause string, value interface{}) SetEntry {
	return SetEntry{include: condition, clause: clause, value: value}
}
