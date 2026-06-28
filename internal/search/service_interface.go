package search

import "context"

// SearchServiceInterface defines the business methods exposed by the search module.
//
// The Handler depends on this interface rather than the concrete *SearchService, enabling unit tests
// for the handler independent of the service implementation, and also supporting nil injection
// when the search service is unavailable.
type SearchServiceInterface interface {
	Search(ctx context.Context, keyword string, size int, tagsCSV, after string, currentUserID *uint64) (*SearchResponse, error)
	Suggest(ctx context.Context, prefix string, size int) ([]string, error)
}

// Compile-time assertion: *SearchService implements SearchServiceInterface.
var _ SearchServiceInterface = (*SearchService)(nil)
