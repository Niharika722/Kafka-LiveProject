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

var processedShipments = sync.Map{} // To ensure idempotency

// StartShipperConsumer processes order picked and packed events from a Kafka topic
func StartShipperConsumer(brokerAddress, orderPickedAndPackedTopic, notificationTopic, deadLetterQueueTopic string) {
	log.Printf("Starting Shipper Consumer for topic: %s", orderPickedAndPackedTopic)

	// Create Kafka reader to consume events from OrderPickedAndPacked topic
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{brokerAddress},
		Topic:       orderPickedAndPackedTopic,
		MinBytes:    10e3,
		MaxBytes:    10e6,
		StartOffset: kafka.LastOffset,
	})
	defer reader.Close()

	// Kafka writer for publishing events to Notification topic
	notificationWriter := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    notificationTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer notificationWriter.Close()

	// Kafka writer for publishing error events to Dead Letter Queue
	dlqWriter := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    deadLetterQueueTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer dlqWriter.Close()

	// Continuously consume messages from OrderPickedAndPacked topic
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		msg, err := reader.ReadMessage(ctx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			log.Printf("Error reading order picked and packed message: %v", err)
			continue
		}

		log.Printf("Received order picked and packed message: %s", string(msg.Value))

		// Deserialize the message into ShipperEvent struct
		var event ShipperEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Unmarshal error: %v", err)
			PublishErrorEvent(dlqWriter, fmt.Sprintf("Invalid order picked and packed format: %v", err), string(msg.Value))
			continue
		}

		// Idempotency check to avoid processing the same order twice
		if _, exists := processedShipments.Load(event.OrderID); exists {
			log.Printf("Duplicate order picked and packed for OrderID %s – skipping", event.OrderID)
			continue
		}
		processedShipments.Store(event.OrderID, true)

		// Simulate business logic for shipping process
		log.Printf("Processing shipping for OrderID: %s", event.OrderID)

		// Handle event failure scenario
		if event.OrderID == "" || event.CustomerID == "" {
			log.Printf("Invalid data in event for OrderID: %s", event.OrderID)
			PublishErrorEvent(dlqWriter, "Missing required fields in ShipperEvent", string(msg.Value))
			processedShipments.Delete(event.OrderID)
			continue
		}

		// Generate a tracking number if not present
		if event.ShippingInfo.TrackingNum == "" {
			event.ShippingInfo.TrackingNum = fmt.Sprintf("TRK-%s-%d", event.OrderID, time.Now().UnixNano())
		}

		// Successfully processed, now publish to the Notification topic
		notificationEvent := NotificationEvent{
			EventType: "OrderShipped",
			Timestamp: time.Now().Format(time.RFC3339),
			OrderID:   event.OrderID,
			Message:   fmt.Sprintf("Your order %s has been shipped! Tracking number: %s", event.OrderID, event.ShippingInfo.TrackingNum),
		}

		// Publish event to the Notification topic
		notificationEventJSON, err := json.Marshal(notificationEvent)
		if err != nil {
			log.Printf("Error marshaling notification event: %v", err)
			PublishErrorEvent(dlqWriter, fmt.Sprintf("Error marshaling notification: %v", err), string(msg.Value))
			processedShipments.Delete(event.OrderID)
			continue
		}

		writeCtx, writeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer writeCancel()

		if err := notificationWriter.WriteMessages(writeCtx, kafka.Message{
			Value: notificationEventJSON,
		}); err != nil {
			log.Printf("Error publishing notification event: %v", err)
			PublishErrorEvent(dlqWriter, fmt.Sprintf("Error publishing notification: %v", err), string(msg.Value))
			processedShipments.Delete(event.OrderID)
			continue
		}

		log.Printf("Successfully processed shipping and sent notification for OrderID: %s", event.OrderID)
	}
}


