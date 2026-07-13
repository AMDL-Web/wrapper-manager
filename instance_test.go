package main

import "testing"

func TestGetInstancesByAccountReturnsAllMatchingWrappers(t *testing.T) {
	original := Instances
	t.Cleanup(func() { Instances = original })

	Instances = []*WrapperInstance{
		{Id: "first", Account: "same@example.com"},
		{Id: "other", Account: "other@example.com"},
		{Id: "second", Account: "same@example.com"},
	}

	matches := GetInstancesByAccount("same@example.com")
	if len(matches) != 2 {
		t.Fatalf("expected 2 wrappers, got %d", len(matches))
	}
	if matches[0].Id != "first" || matches[1].Id != "second" {
		t.Fatalf("unexpected wrappers: %#v", matches)
	}
}
