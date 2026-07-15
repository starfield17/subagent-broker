package doctor

import (
	"errors"
	"regexp"
	"strings"
)

var credentialValue = regexp.MustCompile(`(?i)(bearer\s+|authorization\s*[:=]\s*(?:bearer\s+)?|(?:api[_-]?key|token|secret|password)\s*[:=]\s*)[^\s,;]+`)

func sanitizeText(value string) string {
	lowerValue := strings.ToLower(value)
	if strings.Contains(lowerValue, "authorization:") || strings.Contains(lowerValue, "authorization=") || strings.Contains(lowerValue, "bearer ") {
		return "[REDACTED]"
	}
	value = credentialValue.ReplaceAllString(value, "${1}[REDACTED]")
	words := strings.Fields(value)
	for i, word := range words {
		lower := strings.ToLower(word)
		if strings.Contains(lower, "token") || strings.Contains(lower, "secret") || strings.Contains(lower, "api_key") || strings.Contains(lower, "apikey") || strings.Contains(lower, "password") {
			words[i] = "[REDACTED]"
		}
	}
	return strings.Join(words, " ")
}

func redactLog(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	lines := strings.Split(value, "\n")
	for i, line := range lines {
		lower := strings.ToLower(line)
		if strings.Contains(lower, "token") || strings.Contains(lower, "secret") || strings.Contains(lower, "api_key") || strings.Contains(lower, "apikey") || strings.Contains(lower, "password") || strings.Contains(lower, "authorization") || strings.Contains(lower, "bearer") {
			lines[i] = "[REDACTED]"
		} else {
			lines[i] = sanitizeText(line)
		}
	}
	return strings.Join(lines, "\n")
}

// sanitizeRunResult is applied before both persistence and CLI serialization.
// Keeping the projection at the Doctor boundary ensures an adapter warning or
// termination error cannot bypass the artifact redaction rules.
func sanitizeRunResult(result RunResult) RunResult {
	result.Harnesses = append([]HarnessResult(nil), result.Harnesses...)
	for index := range result.Harnesses {
		item := &result.Harnesses[index]
		item.Warnings = sanitizeStrings(item.Warnings)
		item.Errors = sanitizeStrings(item.Errors)
		item.Probe.Warnings = sanitizeStrings(item.Probe.Warnings)
		item.RuntimeIdentity.Warnings = sanitizeStrings(item.RuntimeIdentity.Warnings)
		item.Cleanup.AdapterTerminateError = sanitizeText(item.Cleanup.AdapterTerminateError)
		item.Cleanup.Errors = sanitizeStrings(item.Cleanup.Errors)
		if len(item.Stages) > 0 {
			stages := make(map[string]StageResult, len(item.Stages))
			for name, stage := range item.Stages {
				stage.Error = sanitizeText(stage.Error)
				stages[name] = stage
			}
			item.Stages = stages
		}
		item.normalizedEventsLog = redactLog(item.normalizedEventsLog)
		item.stderrLog = redactLog(item.stderrLog)
	}
	return result
}

// SanitizeResult returns the redacted projection used at the Doctor/CLI
// boundary. It is exported so callers that inject a Runner cannot bypass the
// same secret-hygiene rules used by the production runner.
func SanitizeResult(result RunResult) RunResult {
	return sanitizeRunResult(result)
}

// SanitizeError removes credential-shaped values before an aggregate Doctor
// error is rendered by a command boundary.
func SanitizeError(err error) error {
	if err == nil {
		return nil
	}
	return errors.New(sanitizeText(err.Error()))
}
