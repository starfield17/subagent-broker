package doctor

import (
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
)

func TestInventorySortsDescriptors(t *testing.T) {
	items := Inventory([]adapter.Descriptor{{Name: "z"}, {Name: "a"}})
	if len(items) != 2 || items[0].Harness != "a" {
		t.Fatalf("unexpected inventory: %+v", items)
	}
}
