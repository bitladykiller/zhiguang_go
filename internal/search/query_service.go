package search

const suggestQueryName = "title-suggest"

type searchResultPayload struct {
	Hits struct {
		Hits []searchHit `json:"hits"`
	} `json:"hits"`
}

type searchHit struct {
	Source    SearchIndexDoc      `json:"_source"`
	Sort      []interface{}       `json:"sort"`
	Highlight map[string][]string `json:"highlight"`
}

type suggestResultPayload struct {
	Suggest map[string][]suggestEntry `json:"suggest"`
}

type suggestEntry struct {
	Options []suggestOption `json:"options"`
}

type suggestOption struct {
	Text string `json:"text"`
}
