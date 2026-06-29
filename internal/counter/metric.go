package counter

func isUserMetric(metric string) bool {
	return metric == indexToName[IdxFollowing] ||
		metric == indexToName[IdxFollower] ||
		metric == indexToName[IdxPosts]
}
