package counter

func isUserMetric(metric string) bool {
	return metric == "following" || metric == "follower" || metric == "posts"
}
