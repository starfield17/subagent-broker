package verify

import (
	"sort"

	"github.com/vnai/subagent-broker/internal/scope"
)

type FileAttribution struct {
	Path   string   `json:"path"`
	Owners []string `json:"owners"`
}

type ScopeAudit struct {
	Authorized     []FileAttribution `json:"authorized"`
	Unauthorized   []string          `json:"unauthorized"`
	OwnerUncertain []FileAttribution `json:"owner_uncertain"`
}

func AuditScopes(changedFiles []string, leases map[string][]string) (ScopeAudit, error) {
	var result ScopeAudit
	files := append([]string(nil), changedFiles...)
	sort.Strings(files)
	for _, file := range files {
		owners, err := scope.CoveringOwners(file, leases)
		if err != nil {
			return ScopeAudit{}, err
		}
		switch len(owners) {
		case 0:
			result.Unauthorized = append(result.Unauthorized, file)
		case 1:
			result.Authorized = append(result.Authorized, FileAttribution{Path: file, Owners: owners})
		default:
			result.OwnerUncertain = append(result.OwnerUncertain, FileAttribution{Path: file, Owners: owners})
		}
	}
	return result, nil
}
