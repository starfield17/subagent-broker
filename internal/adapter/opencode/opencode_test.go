package opencode

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
)

func TestCollectFinalFromSessionMessages(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/session/ses-fixture/message" {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("content-type", "application/json")
		_, _ = writer.Write([]byte(`[{"info":{"role":"assistant","tokens":{"input":3,"output":4},"cost":0.01},"parts":[{"type":"text","text":"{\"schema_version\":\"v1alpha1\",\"task_id\":\"t\",\"worker_id\":\"w\",\"status\":\"succeeded\",\"summary\":\"done\",\"work_completed\":[\"done\"],\"files_changed\":[],\"no_files_changed_reason\":\"fixture\",\"validation\":[{\"command\":\"fixture\",\"passed\":true}],\"remaining_work\":[],\"blocking_issues\":[],\"risks\":[],\"handoff_notes\":[]}"}]}]`))
	}))
	defer server.Close()
	a := New("")
	state := &sessionState{baseURL: server.URL, directory: t.TempDir(), sessionID: "ses-fixture"}
	if err := a.collectFinal(state); err != nil {
		t.Fatal(err)
	}
	if string(state.final) == "" || state.usage.InputTokens != 3 || state.usage.OutputTokens != 4 {
		t.Fatalf("unexpected final=%q usage=%+v", string(state.final), state.usage)
	}
	if _, err := a.NormalizeEvent(adapter.NativeEvent{Kind: "session.idle", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
}
