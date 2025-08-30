package main

import (
	"fmt"
	"log"

	"github.com/nsqio/go-nsq"
)

func main() {
	config := nsq.NewConfig()

	producer, err := nsq.NewProducer("127.0.0.1:4150", config)
	if err != nil {
		log.Fatal(err)
	}

	topicName := "hello"

	for i := 1; i <= 10; i++ {
		message := fmt.Sprintf("hello => %d", i)

		log.Printf("sending message %d ... \n", i)
		err = producer.Publish(topicName, []byte(message))
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("done message %d\n", i)

	}

	producer.Stop()
}
