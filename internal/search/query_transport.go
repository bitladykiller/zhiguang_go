package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
)

func (s *SearchService) executeSearchRequest(
	ctx context.Context,
	query map[string]interface{},
	target interface{},
	opName string,
) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return err
	}

	res, err := s.client.Search(
		s.client.Search.WithContext(ctx),
		s.client.Search.WithIndex(s.indexName),
		s.client.Search.WithBody(&buf),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("%s failed: %s", opName, string(body))
	}

	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return err
	}
	return nil
}
