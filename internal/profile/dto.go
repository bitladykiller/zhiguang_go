package profile

// ProfilePatchRequest 是 `PATCH /profiles/:id` 的请求体。
type ProfilePatchRequest struct {
	Nickname *string `json:"nickname"`
	Avatar   *string `json:"avatar"`
	Bio      *string `json:"bio"`
	Gender   *string `json:"gender"`
	Birthday *string `json:"birthday"`
	School   *string `json:"school"`
	TagsJson *string `json:"tags_json"`
}
