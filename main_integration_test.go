package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

var (
	topics   = []string{"topic1", "topic2", "topic3"}
	messages = []string{"Asiakaslähtöisyys", "Vastuullisuus", "Uusiutuminen", "Tuloksellisuus", "Yhteistyö"}
)

func Test_MainIntegration(t *testing.T) {
	//time for docker kafka to start running
	time.Sleep(10 * time.Second)
	err := CreateProducerAndPublishMessage(topics, messages)
	if err != nil {

	}
	go MessageStreamer()
	_, prodMsgs := fetchMessgesFromProducedTopic()
	if err != nil {
		t.Fatalf("messages posted in topic4 are not jsons")
	}
	file, _ := ioutil.ReadFile("result.json")
	data := []Message{}
	_ = json.Unmarshal([]byte(file), &data)
	var expectedResultVals []string
	var fetchedResultVals []string
	for _, msg := range data {
		expectedResultVals = append(expectedResultVals, msg.Value)
	}
	for _, fm := range prodMsgs {
		fData := Message{}
		_ = json.Unmarshal([]byte(fm), &fData)
		fetchedResultVals = append(fetchedResultVals, fData.Value)
	}
	sort.Strings(expectedResultVals)
	sort.Strings(fetchedResultVals)
	if !reflect.DeepEqual(expectedResultVals, fetchedResultVals) {
		t.Fatalf("fetched and expected values from topic4 are not same")
	}
}
func fetchMessgesFromProducedTopic() (messageCount int, mess []string) {
	config := sarama.NewConfig()
	config.ClientID = "elisa-consumer"
	config.Consumer.Return.Errors = true

	brokers := []string{"localhost:29092"}

	// Create new consumer
	testCon, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := testCon.Close(); err != nil {
			panic(err)
		}
	}()

	topics := []string{"topic4"}

	consumer, errors := cns(topics, testCon)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0

	// Get signnal for finish
	doneCh := make(chan struct{})
	var val []string
	go func() {
		for {
			select {
			case msg := <-consumer:
				msgCount++
				if msgCount == 5 {
					doneCh <- struct{}{}
				}
				val = append(val, string(msg.Value))
			case consumerError := <-errors:
				msgCount++
				fmt.Println("Received consumerError ", string(consumerError.Topic), string(consumerError.Partition), consumerError.Err)
				doneCh <- struct{}{}
			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")
	return msgCount, val

}

func cns(topics []string, master sarama.Consumer) (chan *sarama.ConsumerMessage, chan *sarama.ConsumerError) {
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
		fmt.Println(" Start consuming topic ", topic)
		go func(topic string, consumer sarama.PartitionConsumer) {
			for {
				select {
				case consumerError := <-consumer.Errors():
					errors <- consumerError
					fmt.Println("consumerError: ", consumerError.Err)

				case msg := <-consumer.Messages():
					consumers <- msg
				}
			}

		}(topic, consumer)
	}

	return consumers, errors
}
