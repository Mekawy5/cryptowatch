package producer

import (
	"github.com/Shopify/sarama"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	brokerList = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9093").Strings()
	topic      = kingpin.Flag("topic", "Topic name").Default("topic1").String()
	maxRetry   = kingpin.Flag("maxRetry", "Retry limit").Default("5").Int()
)

// Producer object have kafka producer and the topic
type Producer struct {
	sarama.SyncProducer
	t string
}

// NewProducer creates producer object
func NewProducer() *Producer {
	kingpin.Parse()
	conf := sarama.NewConfig()
	conf.Producer.RequiredAcks = sarama.WaitForLocal //i have no replicas so wait for all is good
	conf.Producer.Retry.Max = *maxRetry
	conf.Producer.Return.Successes = true

	prod, err := sarama.NewSyncProducer(*brokerList, conf)
	if err != nil {
		panic(err)
	}

	return &Producer{
		prod,
		*topic,
	}
}

// Produce message to defined topic
func (p *Producer) Produce(msg string) (int32, int64) {
	m := &sarama.ProducerMessage{Topic: p.t, Value: sarama.StringEncoder(msg)}
	prt, off, err := p.SendMessage(m)
	if err != nil {
		panic(err)
	}

	return prt, off
}
