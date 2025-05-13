package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

var processedOrders = sync.Map{} // To track processed orders for idempotency

func StartInventoryConsumer(brokerAddress, orderReceivedTopic, orderConfirmedTopic, deadLetterQueueTopic string) {
	// Initialize error tracking
	atomic.StoreUint64(&errorCount, 0)
	lastErrorReset = time.Now()
	
	// Start a goroutine to periodically publish the errors-per-minute KPI
	go trackErrorsPerMinute(brokerAddress)

	log.Printf("Starting Inventory consumer...")
	
	// Create a reader for the order-received topic
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{brokerAddress},
		Topic:       orderReceivedTopic,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		StartOffset: kafka.LastOffset,
	})
	defer reader.Close()
	
	// Create a writer for the order-confirmed topic
	confirmedWriter := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    orderConfirmedTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer confirmedWriter.Close()
	
	// Create a writer for the error-events topic
	errorWriter := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    deadLetterQueueTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer errorWriter.Close()
	
	log.Printf("Starting Inventory consumer from order-received to order-confirmed")
	
	// Process messages in a loop
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		m, err := reader.ReadMessage(ctx)
		cancel()
		
		if err != nil {
			// Handle context deadline exceeded silently
			if errors.Is(err, context.DeadlineExceeded) {
				// This is normal - just means no messages were available within the timeout
				continue
			}
			
			// Log other errors
			log.Printf("Error reading message: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		
		// Log the received message
		log.Printf("Received message: %s", string(m.Value))
		
		// Process the message directly
		var event OrderEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("Error unmarshalling message: %v", err)
			
			// Publish error event to the DeadLetterQueue topic
			PublishErrorEvent(errorWriter, fmt.Sprintf("Failed to unmarshal message: %v", err), string(m.Value))
			
			// Increment error count for KPI tracking
			atomic.AddUint64(&errorCount, 1)
			continue
		}
		
		// Process the order with idempotence handling
		log.Printf("Processing order: %s", event.OrderID)
		
		// Check if this order has already been processed (idempotence check)
		if _, exists := processedOrders.Load(event.OrderID); exists {
			log.Printf("Duplicate order detected: %s - skipping processing", event.OrderID)
			log.Printf("Successfully processed message at offset %d (duplicate)", m.Offset)
			continue
		}
		
		// Mark this order as processed
		processedOrders.Store(event.OrderID, true)
		
		// Create confirmed event
		confirmedEvent := struct {
			EventType string `json:"eventType"`
			Timestamp string `json:"timestamp"`
			OrderID   string `json:"orderId"`
			Status    string `json:"status"`
		}{
			EventType: "OrderConfirmed",
			Timestamp: time.Now().Format(time.RFC3339),
			OrderID:   event.OrderID,
			Status:    "confirmed",
		}
		
		// Marshal the confirmed event
		confirmedJSON, err := json.Marshal(confirmedEvent)
		if err != nil {
			log.Printf("Error marshalling confirmed event: %v", err)
			
			// Publish error event to the DeadLetterQueue topic
			PublishErrorEvent(errorWriter, fmt.Sprintf("Failed to marshal confirmed event: %v", err), string(m.Value))
			
			// Increment error count for KPI tracking
			atomic.AddUint64(&errorCount, 1)
			continue
		}
		
		// Generate a unique key for the message
		confirmedKey := fmt.Sprintf("confirmed-%s-%d", event.OrderID, time.Now().UnixNano())
		
		// Publish to order-confirmed topic
		log.Printf("Publishing confirmed event for order: %s to topic: order-confirmed", event.OrderID)
		if err := confirmedWriter.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(confirmedKey),
			Value: confirmedJSON,
		}); err != nil {
			log.Printf("Error publishing to order-confirmed topic: %v", err)
			
			// Publish error event to the DeadLetterQueue topic
			PublishErrorEvent(errorWriter, fmt.Sprintf("Failed to publish confirmed event: %v", err), string(m.Value))
			
			// Increment error count for KPI tracking
			atomic.AddUint64(&errorCount, 1)
			
			// Remove from processed orders since we couldn't complete the processing
			processedOrders.Delete(event.OrderID)
			continue
		}
		
		log.Printf("Successfully published confirmed event to topic order-confirmed for order %s with key %s", 
			event.OrderID, confirmedKey)
			
		// We're not using consumer groups, so we don't need to commit offsets
		// Just log that we've processed the message successfully
		log.Printf("Successfully processed message at offset %d", m.Offset)
	}
}


