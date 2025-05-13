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

var processedWarehouse = sync.Map{} // To ensure idempotency


// StartWarehouseConsumer processes order confirmed events from a Kafka topic
func StartWarehouseConsumer(brokerAddress, orderConfirmedTopic, orderPickedAndPackedTopic, notificationTopic, deadLetterQueueTopic string) {
	// Start a goroutine to periodically publish the processing latency KPI
	go trackProcessingLatency(brokerAddress)
	log.Printf("Starting Warehouse Consumer for topic: %s", orderConfirmedTopic)

	// Create Kafka reader to consume events from OrderConfirmed topic
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{brokerAddress},
		Topic:       orderConfirmedTopic,
		MinBytes:    10e3,
		MaxBytes:    10e6,
		StartOffset: kafka.LastOffset,
	})
	defer reader.Close()

	//kafka writer for publishing events to OrderPickedAndPacked topic
	orderPickedAndPackedWriter := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    orderPickedAndPackedTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer orderPickedAndPackedWriter.Close()

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

	// Continuously consume messages from OrderConfirmed topic
	for {
		// Record start time for latency measurement
		startTime := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		msg, err := reader.ReadMessage(ctx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			log.Printf("Error reading order confirmed message: %v", err)
			continue
		}

		log.Printf("Received order confirmed message: %s", string(msg.Value))

		// Deserialize the message into WarehouseEvent struct
		var event WarehouseEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Unmarshal error: %v", err)
			PublishErrorEvent(dlqWriter, fmt.Sprintf("Invalid order confirmed format: %v", err), string(msg.Value))
			continue
		}

		// Idempotency check to avoid processing the same order twice
		if _, exists := processedWarehouse.Load(event.OrderID); exists {
			log.Printf("Duplicate order confirmed for OrderID %s – skipping", event.OrderID)
			continue
		}
		processedWarehouse.Store(event.OrderID, true)

		// Simulate business logic for order processed
		log.Printf("Processing order confirmation for OrderID: %s - Status: %s", event.OrderID, event.Status)

		// Handle event failure scenario
		if event.OrderID == "" || event.EventType == "" {
			log.Printf("Invalid data in event for OrderID: %s", event.OrderID)
			PublishErrorEvent(dlqWriter, "Missing required fields in WarehouseEvent", string(msg.Value))
			processedWarehouse.Delete(event.OrderID)
			continue
		}

		// Create an order picked and packed event
		pickedAndPackedEvent := ShipperEvent{
			EventType:  "OrderPickedAndPacked",
			Timestamp:  time.Now().Format(time.RFC3339),
			OrderID:    event.OrderID,
			CustomerID: "customer-456", // In a real system, this would come from the order data
			ShippingInfo: struct {
				Address     string `json:"address"`
				City        string `json:"city"`
				PostalCode  string `json:"postalCode"`
				TrackingNum string `json:"trackingNum"`
			}{
				Address:    "123 Main St",
				City:       "New York",
				PostalCode: "10001",
			},
		}

		// Marshal the picked and packed event
		pickedAndPackedJSON, err := json.Marshal(pickedAndPackedEvent)
		if err != nil {
			log.Printf("Error marshaling picked and packed event: %v", err)
			PublishErrorEvent(dlqWriter, fmt.Sprintf("Error marshaling picked and packed event: %v", err), string(msg.Value))
			processedWarehouse.Delete(event.OrderID)
			continue
		}

		// Create a new context for publishing to order-picked-and-packed topic
		publishPickedCtx, publishPickedCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer publishPickedCancel()

		// Publish to order-picked-and-packed topic
		log.Printf("Publishing order picked and packed event for OrderID: %s", event.OrderID)
		if err := orderPickedAndPackedWriter.WriteMessages(publishPickedCtx, kafka.Message{
			Value: pickedAndPackedJSON,
		}); err != nil {
			log.Printf("Error publishing order picked and packed event: %v", err)
			PublishErrorEvent(dlqWriter, fmt.Sprintf("Error publishing order picked and packed: %v", err), string(msg.Value))
			processedWarehouse.Delete(event.OrderID)
			continue
		}

		// Successfully processed, now also publish to the Notification topic
		notificationEvent := NotificationEvent{
			EventType: "WarehouseProcessed",
			Timestamp: time.Now().Format(time.RFC3339),
			OrderID:   event.OrderID,
			Message:   fmt.Sprintf("Order %s has been processed by the warehouse", event.OrderID),
		}

		// Publish event to the Notification topic
		notificationEventJSON, err := json.Marshal(notificationEvent)
		if err != nil {
			log.Printf("Error marshaling notification event: %v", err)
			PublishErrorEvent(dlqWriter, fmt.Sprintf("Error marshaling notification: %v", err), string(msg.Value))
			processedWarehouse.Delete(event.OrderID)
			continue
		}

		// Create a new context for publishing notifications
		publishCtx, publishCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer publishCancel()
		if err := notificationWriter.WriteMessages(publishCtx, kafka.Message{
			Value: notificationEventJSON,
		}); err != nil {
			log.Printf("Error publishing notification event: %v", err)
			PublishErrorEvent(dlqWriter, fmt.Sprintf("Error publishing notification: %v", err), string(msg.Value))
			processedWarehouse.Delete(event.OrderID)
			continue
		}

		// Calculate and record processing latency
		latencyMs := float64(time.Since(startTime).Milliseconds())
		recordLatency(latencyMs)

		log.Printf("Successfully processed and sent notification for OrderID: %s (latency: %.2fms)", event.OrderID, latencyMs)
	}
}