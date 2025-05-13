package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

var processedNotifications = sync.Map{} // To ensure idempotency

// StartNotificationConsumer processes notification events from a Kafka topic
func StartNotificationConsumer(brokerAddress, notificationTopic, deadLetterQueueTopic string) {
	log.Printf("Starting Notification Consumer for topic: %s", notificationTopic)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{brokerAddress},
		Topic:       notificationTopic,
		MinBytes:    10e3,
		MaxBytes:    10e6,
		StartOffset: kafka.LastOffset,
	})
	defer reader.Close()

	dlqWriter := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    deadLetterQueueTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer dlqWriter.Close()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		msg, err := reader.ReadMessage(ctx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			log.Printf("Error reading notification message: %v", err)
			continue
		}

		log.Printf("Received notification: %s", string(msg.Value))

		var event NotificationEvent
		// Clean the message value to remove any trailing control characters
		cleanedMsg := CleanMessageValue(msg.Value)
		if err := json.Unmarshal(cleanedMsg, &event); err != nil {
			log.Printf("Unmarshal error: %v", err)
			PublishErrorEvent(dlqWriter, fmt.Sprintf("Invalid notification format: %v", err), string(msg.Value))
			continue
		}

		// Idempotency check
		if _, exists := processedNotifications.Load(event.OrderID); exists {
			log.Printf("Duplicate notification for OrderID %s – skipping", event.OrderID)
			continue
		}
		processedNotifications.Store(event.OrderID, true)

		// Handle the event (mock business logic)
		log.Printf("Sending notification for OrderID: %s - Message: %s", event.OrderID, event.Message)

		// Simulate success/failure
		if event.OrderID == "" || event.Message == "" {
			log.Printf("Invalid data in event for OrderID: %s", event.OrderID)
			PublishErrorEvent(dlqWriter, "Missing required fields in NotificationEvent", string(msg.Value))
			processedNotifications.Delete(event.OrderID)
			continue
		}

		log.Printf("Successfully processed notification for OrderID: %s", event.OrderID)
	}
}
