package counter

// isUserMetric 判断指标是否为用户维度而非实体维度。
//
// 用户维度的指标（following、follower）使用 `user:{userID}` 作为 SDS key，
// 实体维度的指标（like、fav）使用 `{entityType}:{entityID}` 作为 SDS key。
//
// 注意: 此函数仅在测试中使用，生产路径已改用 hasAnyBit(mode, UserModeBits) 判断。
func isUserMetric(metric string) bool {
	return metric == "following" || metric == "follower" || metric == "posts"
}
