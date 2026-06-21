package counter

// isUserMetric 判断指标是否为用户维度而非实体维度。
//
// 用户维度的指标（following、follower）使用 `user:{userID}` 作为 SDS key，
// 实体维度的指标（like、fav）使用 `{entityType}:{entityID}` 作为 SDS key。
func isUserMetric(metric string) bool {
	return metric == "following" || metric == "follower" || metric == "posts"
}
