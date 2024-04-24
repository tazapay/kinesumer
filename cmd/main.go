package main

import (
	"context"
	"fmt"
	"time"

	"github.com/tazapay/kinesumer"
)

func main() {
	client, err := kinesumer.NewKinesumer(
		context.Background(),
		&kinesumer.Config{
			App:              "myapp",
			KinesisRegion:    "ap-southeast-1",
			DynamoDBRegion:   "ap-southeast-1",
			DynamoDBTable:    "kinesumer-state-store",
			ScanLimit:        1500,
			ScanTimeout:      2 * time.Second,
			KinesisEndpoint:  "http://localhost:14566",
			DynamoDBEndpoint: "http://localhost:14566",
		},
	)
	if err != nil {
		fmt.Println("failed to init", err)
		return
	}

	go func() {
		for err := range client.Errors() {
			// Error handling.
			fmt.Println("!!!", err)
		}
	}()

	// Consume multiple streams.
	// You can refresh the streams with `client.Refresh()` method.
	records, err := client.Consume(context.Background(), []string{"events"})
	if err != nil {
		fmt.Println("@@@", err)
		return
	}

	for record := range records {
		fmt.Printf("record###: %v\n", record)
	}
}
