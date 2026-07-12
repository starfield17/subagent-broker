package interaction

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestWorkerServerAdvertisesBrokerTools(t *testing.T) {
	input := strings.NewReader("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}\n{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}\n")
	var output bytes.Buffer
	if err := (WorkerServer{}).serve(context.Background(), input, &output); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("unexpected output: %s", output.String())
	}
	var response struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(lines[1]), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Result.Tools) != 2 || response.Result.Tools[0].Name != "ask_main_agent" || response.Result.Tools[1].Name != "request_scope_expansion" {
		t.Fatalf("unexpected tools: %+v", response.Result.Tools)
	}
}
