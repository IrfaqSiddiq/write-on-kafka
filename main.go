package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Create a new Kafka writer.
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topix",
	})
	defer w.Close()

	// Write a message.
	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Hello abc!"),
		},
	)

	if err != nil {
		fmt.Println("***Error", err)
		panic(err)
	}

	fmt.Println("Message sent")
}
