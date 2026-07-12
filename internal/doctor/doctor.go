package doctor

import (
	"sort"

	"github.com/vnai/subagent-broker/internal/adapter"
)

type Item struct {
	Harness       adapter.HarnessName  `json:"harness"`
	Implemented   bool                 `json:"implemented"`
	Compatibility string               `json:"compatibility"`
	Capabilities  adapter.Capabilities `json:"capabilities"`
}

func Inventory(descriptors []adapter.Descriptor) []Item {
	items := make([]Item, 0, len(descriptors))
	for _, d := range descriptors {
		items = append(items, Item{Harness: d.Name, Implemented: d.RuntimeImplemented, Compatibility: d.Compatibility, Capabilities: d.Capabilities})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Harness < items[j].Harness })
	return items
}
