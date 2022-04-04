package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
	"strings"
	"time"
)

var (
	kafkaBrokers = []string{"localhost:29092"}
)

type Message struct {
	Value string
}

func main() {
	MessageStreamer()
}

func MessageStreamer() {
	config := sarama.NewConfig()
	config.ClientID = "elisa-consumer"
	config.Consumer.Return.Errors = true

	brokers := []string{"localhost:29092"}

	// Create new consumer
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	topics := []string{"topic1", "topic2", "topic3"}

	consumer, errors := consume(topics, master)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0

	// Get signnal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case _ = <-consumer:
				msgCount++
			case _ = <-errors:
				msgCount++
				doneCh <- struct{}{}
			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")

}

func consume(topics []string, master sarama.Consumer) (chan *sarama.ConsumerMessage, chan *sarama.ConsumerError) {
	consumers := make(chan *sarama.ConsumerMessage)
	errors := make(chan *sarama.ConsumerError)
	for _, topic := range topics {
		if strings.Contains(topic, "__consumer_offsets") {
			continue
		}
		partitions, _ := master.Partitions(topic)
		// this only consumes partition no 1, you would probably want to consume all partitions
		consumer, err := master.ConsumePartition(topic, partitions[0], sarama.OffsetOldest)
		if nil != err {
			fmt.Printf("Topic %v Partitions: %v", topic, partitions)
			panic(err)
		}
		producer, err := initProducer()
		if err != nil {
			fmt.Println("Error producer: ", err.Error())
		}
		go func(topic string, consumer sarama.PartitionConsumer) {
			for {
				select {
				case consumerError := <-consumer.Errors():
					errors <- consumerError
					fmt.Println("consumerError: ", consumerError.Err)

				case msg := <-consumer.Messages():
					consumers <- msg
					message := &Message{Value: string(msg.Value)}
					b, _ := json.Marshal(message)
					publish(string(b), "topic4", producer)
					fmt.Println("Got message on topic at", topic, string(msg.Value), time.Now())
				}
			}

		}(topic, consumer)
	}

	return consumers, errors
}

func CreateProducerAndPublishMessage(topics, messages []string) error {
	producer, err := initProducer()
	if err != nil {
		fmt.Println("Error producer: ", err.Error())
		return err
	}
	for i, message := range messages {
		l := len(topics)
		if i < l {
			err = publish(message, topics[i], producer)
		} else {
			err = publish(message, topics[i%l], producer)
		}
	}
	return err
}

func initProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	prd, err := sarama.NewSyncProducer(kafkaBrokers, config)

	return prd, err
}

func publish(message, topic string, producer sarama.SyncProducer) error {
	// publish sync
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	_, _, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println("Error publish on topic: ", topic, err.Error())
		return err
	}
	return nil
}
