package notify

import (
	"bytes"
	"context"
	"net/http"
	"time"

	"github.com/binbinpeng123/blitz/pkg/conf"
	"k8s.io/apimachinery/pkg/util/json"
)

// SendFS 发送飞书通知
// 接口doc: https://www.feishu.cn/hc/zh-CN/articles/360024984973
func SendFS(ctx context.Context, msg string) {
	url := conf.Conf.FeishuRobotURL

	data := struct {
		MsgType string `json:"msg_type"`
		Content struct {
			Text string `json:"text"`
		} `json:"content"`
	}{
		MsgType: "text",
		Content: struct {
			Text string `json:"text"`
		}(struct{ Text string }{Text: msg}),
	}
	buf, _ := json.Marshal(data)
	request, _ := http.NewRequest("POST", url, bytes.NewBuffer(buf))
	cli := &http.Client{Timeout: time.Second * 3}
	cli.Do(request)
}
