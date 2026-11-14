package client

import (
	"context"
	"strings"

	"github.com/binbinpeng123/blitz/pkg/conf"
	kafkaGo "github.com/segmentio/kafka-go"
	"gitlab.hobot.cc/aidi/slardar/log"
	"k8s.io/apimachinery/pkg/util/json"
)

var writer kafkaGo.Writer

type KafkaTopic string

const (
	DagStatusTopic KafkaTopic = "computing_dag_status"
	JobStatusTopic KafkaTopic = "computing_job_status"
)

func KafkaInit() {
	servers := conf.Conf.KafkaServers
	if servers == "" {
		panic("kafka_servers not found in conf")
	}
	writer = *kafkaGo.NewWriter(kafkaGo.WriterConfig{
		Brokers:  strings.Split(servers, ","),
		Balancer: &kafkaGo.Hash{},
	})
}

type KafkaStatus struct {
	// dag/job id
	Id string
	// dag/job name
	Name string
	// dag/job 状态
	Status string
	// 结束原因
	Reason string
}

func Produce(status KafkaStatus, topic KafkaTopic) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	data, _ := json.Marshal(status)
	msg := kafkaGo.Message{
		Topic: string(topic),
		Key:   []byte(status.Id),
		Value: data,
	}
	if err = writer.WriteMessages(ctx, msg); err != nil {
		log.Get(ctx).Errorf("Kafka [Produce] failed status: %+v err %+v", status, err)
	} else {
		log.Get(ctx).Infof("Kafka [Produce] success status: %+v", status)
	}

	return
}
