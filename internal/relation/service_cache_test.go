package relation

import (
	"reflect"
	"testing"
)

func TestToLongListSkipsInvalidMembers(t *testing.T) {
	service := &RelationService{}

	got := service.toLongList("1001, invalid, 1002, ,1003")
	want := []uint64{1001, 1002, 1003}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("toLongList() = %v, want %v", got, want)
	}
}

func TestToIDListSkipsInvalidMembers(t *testing.T) {
	service := &RelationService{}

	got := service.toIDList([]string{"2001", "oops", "2002", "2003"})
	want := []uint64{2001, 2002, 2003}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("toIDList() = %v, want %v", got, want)
	}
}
