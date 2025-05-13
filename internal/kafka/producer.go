package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func PublishEvent(ctx context.Context, message string, brokerAddress string, topic string) error {
	log.Printf("Attempting to publish message to topic %s at broker %s", topic, brokerAddress)
	
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{brokerAddress},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		// Using default RequiredAcks setting
		Async: false, // Use synchronous writes for reliability
	})
	defer writer.Close()

	// Add a unique key to ensure proper partitioning
	msgKey := fmt.Sprintf("key-%d", time.Now().UnixNano())
	
	err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(msgKey),
		Value: []byte(message),
		Time:  time.Now(),
	})
	if err != nil {
		log.Printf("Failed to write message to topic %s: %v", topic, err)
		return err
	}

	log.Printf("Message published to topic %s successfully with key %s", topic, msgKey)
	return nil
}

// Helper function to publish error events to the DeadLetterQueue topic
func PublishErrorEvent(errorWriter *kafka.Writer, errorMessage, originalMessage string) {
	// Create error event
	errorEvent := ErrorEvent{
		EventType:       "ProcessingError",
		Timestamp:       time.Now().Format(time.RFC3339),
		Error:           errorMessage,
		OriginalMessage: originalMessage,
	}
	
	// Marshal the error event
	errorJSON, err := json.Marshal(errorEvent)
	if err != nil {
		log.Printf("Error marshalling error event: %v", err)
		return
	}
	
	// Generate a unique key for the message
	errorKey := fmt.Sprintf("error-%d", time.Now().UnixNano())
	
	// Publish to error-events topic
	log.Printf("Publishing error event to topic: error-events - %s", errorMessage)
	if err := errorWriter.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(errorKey),
		Value: errorJSON,
	}); err != nil {
		log.Printf("Error publishing to error-events topic: %v", err)
		return
	}
	
	log.Printf("Successfully published error event to topic error-events with key %s", errorKey)
}

// PublishKPIEvent publishes a KPI event to the specified Kafka topic
func PublishKPIEvent(writer *kafka.Writer, kpiName, metricName string, metricValue float64, serviceName string, metadata interface{}) error {
	// Create KPI event
	kpiEvent := KPIEvent{
		EventType:   "KPIEvent",
		Timestamp:   time.Now().Format(time.RFC3339),
		KPIName:     kpiName,
		MetricName:  metricName,
		MetricValue: metricValue,
		ServiceName: serviceName,
		Metadata:    metadata,
	}
	
	// Marshal the KPI event
	kpiJSON, err := json.Marshal(kpiEvent)
	if err != nil {
		log.Printf("Error marshalling KPI event: %v", err)
		return err
	}
	
	// Generate a unique key for the message
	kpiKey := fmt.Sprintf("kpi-%s-%s-%d", kpiName, metricName, time.Now().UnixNano())
	
	// Publish to KPI topic
	log.Printf("Publishing KPI event: %s - %s = %f", kpiName, metricName, metricValue)
	if err := writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(kpiKey),
		Value: kpiJSON,
	}); err != nil {
		log.Printf("Error publishing KPI event: %v", err)
		return err
	}
	
	log.Printf("Successfully published KPI event with key %s", kpiKey)
	return nil
}
