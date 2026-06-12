package search

import (
	"context"

	"github.com/zhiguang/app/internal/knowpost"
)

func (s *SearchService) buildSearchResponse(
	ctx context.Context,
	hits []searchHit,
	size int,
	currentUserID *uint64,
) *SearchResponse {
	items := make([]knowpost.FeedItemResponse, 0, len(hits))
	for _, hit := range hits {
		items = append(items, s.buildFeedItemFromHit(ctx, hit, currentUserID))
	}

	var nextAfter *string
	if len(hits) > 0 {
		lastSort := hits[len(hits)-1].Sort
		if len(lastSort) > 0 {
			cursor := encodeAfter(lastSort)
			nextAfter = &cursor
		}
	}

	return &SearchResponse{
		Items:     items,
		NextAfter: nextAfter,
		HasMore:   len(items) >= size,
	}
}

func (s *SearchService) buildFeedItemFromHit(
	ctx context.Context,
	hit searchHit,
	currentUserID *uint64,
) knowpost.FeedItemResponse {
	source := hit.Source
	description := source.Description
	if snippet := buildSnippet(hit.Highlight); snippet != "" {
		description = snippet
	}

	var coverImage *string
	if len(source.ImgURLs) > 0 {
		first := source.ImgURLs[0]
		coverImage = &first
	}

	liked, faved := s.userFlags(ctx, currentUserID, source.ID)
	return knowpost.FeedItemResponse{
		ID:             source.ID,
		Title:          stringPtrOrNil(source.Title),
		Description:    stringPtrOrNil(description),
		CoverImage:     coverImage,
		Tags:           source.Tags,
		AuthorAvatar:   source.AuthorAvatar,
		AuthorNickname: source.AuthorName,
		TagJson:        source.AuthorTagJSON,
		LikeCount:      source.LikeCount,
		FavoriteCount:  source.FavCount,
		Liked:          liked,
		Faved:          faved,
		IsTop:          boolPtr(source.IsTop),
	}
}
