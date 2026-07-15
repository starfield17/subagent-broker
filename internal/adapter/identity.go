package adapter

import (
	"encoding/json"
	"strings"
)

// RuntimeIdentityFields extracts only explicit native protocol fields. It is
// intentionally limited to provider/model key names and never treats an
// executable, adapter name, or requested model as an observation.
func RuntimeIdentityFields(payload []byte) (provider, model string) {
	var value any
	if len(payload) == 0 || json.Unmarshal(payload, &value) != nil {
		return "", ""
	}
	var visit func(any)
	visit = func(current any) {
		if provider != "" && model != "" {
			return
		}
		switch typed := current.(type) {
		case map[string]any:
			for key, child := range typed {
				lower := strings.ToLower(strings.ReplaceAll(key, "_", ""))
				switch lower {
				case "provider", "providerid", "modelprovider":
					if value, ok := child.(string); ok && strings.TrimSpace(value) != "" && provider == "" {
						provider = strings.TrimSpace(value)
					}
				case "model", "modelid":
					if value, ok := child.(string); ok && strings.TrimSpace(value) != "" && model == "" {
						model = strings.TrimSpace(value)
					}
				}
				visit(child)
			}
		case []any:
			for _, child := range typed {
				visit(child)
			}
		}
	}
	visit(value)
	return provider, model
}
