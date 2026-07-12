package supervisor

import "github.com/vnai/subagent-broker/internal/contracttest"

func contractSteerVerified(harness, version string) bool {
	return contracttest.SteerVerified(harness, version)
}
