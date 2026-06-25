package search

// SearchItem 是搜索结果的条目结构，与 knowpost.FeedItemResponse 字段对齐。
// 这是 search 包自有的类型，无需依赖 knowpost 包。
type SearchItem struct {
	ID             string   `json:"id"`
	Title          *string  `json:"title"`
	Description    *string  `json:"description"`
	CoverImage     *string  `json:"cover_image"`
	Tags           []string `json:"tags"`
	AuthorAvatar   *string  `json:"author_avatar"`
	AuthorNickname string   `json:"author_nickname"`
	TagJson        *string  `json:"tag_json"`
	LikeCount      int64    `json:"like_count"`
	FavoriteCount  int64    `json:"favorite_count"`
	Liked          *bool    `json:"liked,omitempty"`
	Faved          *bool    `json:"faved,omitempty"`
	IsTop          *bool    `json:"is_top,omitempty"`
}

// ApplyLikedFaved fills in the Liked and Faved pointer fields for this item
// from the provided batch lookup maps. Nil maps are safely ignored.
func (item *SearchItem) ApplyLikedFaved(likedMap, favedMap map[string]bool) {
	if likedMap != nil {
		if l, ok := likedMap[item.ID]; ok {
			item.Liked = &l
		}
	}
	if favedMap != nil {
		if f, ok := favedMap[item.ID]; ok {
			item.Faved = &f
		}
	}
}

// SearchResponse 搜索接口的响应结构，字段对齐 Java 版返回。
type SearchResponse struct {
	Items     []SearchItem `json:"items"`
	NextAfter *string      `json:"next_after,omitempty"`
	HasMore   bool         `json:"has_more"`
}

// SuggestField 表示 ES completion suggest 字段结构。
type SuggestField struct {
	Input  []string `json:"input"`
	Weight int      `json:"weight,omitempty"`
}
