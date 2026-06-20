package counter

import (
	"encoding/json"
	"testing"
)

func TestMarshalCounterEventJSON_MatchesStdlib(t *testing.T) {
	evt := &CounterEvent{
		MessageID:  99,
		EntityType: "post",
		EntityID:   "42",
		Metric:     "like",
		Index:      IdxLike,
		UserID:     1001,
		Delta:      1,
	}
	got := MarshalCounterEventJSON(evt)
	want, err := json.Marshal(evt)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestMarshalCounterEventJSON_Escapes(t *testing.T) {
	evt := &CounterEvent{
		MessageID:  1,
		EntityType: `a"b`,
		EntityID:   "1",
		Metric:     "like",
		Index:      0,
		UserID:     1,
		Delta:      -1,
	}
	got := MarshalCounterEventJSON(evt)
	want, _ := json.Marshal(evt)
	if string(got) != string(want) {
		t.Fatalf("got %s want %s", got, want)
	}
}