package fanout

import "strconv"

// Redis 键设计
//
// 混合扩散一共只用四类键，职责严格分离：
//
//	timeline:{userID}    ZSet  收件箱（推）。member=postID，score=发布时间戳（秒）。
//	                           只保存「被推送过来」的帖子，即普通作者的帖子。
//	authorbox:{authorID} ZSet  发件箱（拉）。member=postID，score=发布时间戳（秒）。
//	                           保存该作者最近 N 条帖子，任何作者都写，写入是 O(1)。
//	fanout:celeb         SET   大 V 名单。读路径据此决定哪些关注对象要走拉。
//	z:following:{userID} ZSet  关注列表（relation 模块已有，此处只读）。
//
// WHY 发件箱对所有作者都写，而不只是大 V：
//
//	作者的「大 V 身份」会随粉丝增长而变化。若只有大 V 才写发件箱，
//	一个普通作者升级为大 V 的瞬间，他此前的帖子在发件箱里是空的，
//	而这些帖子又已经不会再被推送——读者会看到一段内容空洞。
//	发件箱写入本身是 O(1)（一次 ZADD + 一次 ZREMRANGEBYRANK），成本可忽略，
//	因此无条件写入，让身份切换随时安全。
//	它同时也是取关清理的数据来源（见 RemoveAuthorFromTimeline）。
const (
	timelineKeyPrefix  = "timeline:"
	authorBoxKeyPrefix = "authorbox:"

	// celebritySetKey 是全局大 V 名单。
	//
	// 大 V 在任何社区里都是少数，因此用一个全局 SET 保存即可，
	// 读路径用「我的关注列表 ∩ 该集合」算出需要拉取的作者。
	celebritySetKey = "fanout:celeb"
)

// timelineKey 返回某个用户收件箱的键。
func timelineKey(userID uint64) string {
	return timelineKeyPrefix + strconv.FormatUint(userID, 10)
}

// authorBoxKey 返回某个作者发件箱的键。
func authorBoxKey(authorID uint64) string {
	return authorBoxKeyPrefix + strconv.FormatUint(authorID, 10)
}
