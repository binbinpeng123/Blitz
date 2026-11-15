package notify

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/binbinpeng123/blitz/pkg/conf"
)

type feishuPayload struct {
	MsgType string `json:"msg_type"`
	Content struct {
		Text string `json:"text"`
	} `json:"content"`
}

func TestSendFS(t *testing.T) {
	var (
		gotMethod string
		gotPath   string
		gotBody   feishuPayload
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	conf.Conf = conf.Config{FeishuRobotURL: srv.URL + "/robot"}
	SendFS(context.Background(), "hello")

	if gotMethod != http.MethodPost {
		t.Fatalf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/robot" {
		t.Fatalf("path = %q, want /robot", gotPath)
	}
	if gotBody.MsgType != "text" {
		t.Fatalf("msg_type = %q, want text", gotBody.MsgType)
	}
	if gotBody.Content.Text != "hello" {
		t.Fatalf("content.text = %q, want hello", gotBody.Content.Text)
	}
}
