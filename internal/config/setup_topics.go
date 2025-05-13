package config

import (
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// SetupTopics creates and verifies Kafka topics
func SetupTopics(brokerAddress string) {
	log.Printf("Setting up Kafka topics at broker: %s", brokerAddress)

	// Topics to create
	topics := []kafka.TopicConfig{
		{
			Topic:             "order-received",
			NumPartitions:     1,
			ReplicationFactor: 1,
			ConfigEntries: []kafka.ConfigEntry{
				{
					ConfigName:  "retention.ms",
					ConfigValue: "259200000", // 3 days
				},
			},
		},
		{
			Topic:             "order-confirmed",
			NumPartitions:     1,
			ReplicationFactor: 1,
			ConfigEntries: []kafka.ConfigEntry{
				{
					ConfigName:  "retention.ms",
					ConfigValue: "259200000", // 3 days
				},
			},
		},
		{
			Topic:             "order-picked-and-packed",
			NumPartitions:     1,
			ReplicationFactor: 1,
			ConfigEntries: []kafka.ConfigEntry{
				{
					ConfigName:  "retention.ms",
					ConfigValue: "259200000", // 3 days
				},
			},
		},
		{
			Topic:             "notification",
			NumPartitions:     1,
			ReplicationFactor: 1,
			ConfigEntries: []kafka.ConfigEntry{
				{
					ConfigName:  "retention.ms",
					ConfigValue: "259200000", // 3 days
				},
			},
		},
		{
			Topic:             "error-events",
			NumPartitions:     1,
			ReplicationFactor: 1,
			ConfigEntries: []kafka.ConfigEntry{
				{
					ConfigName:  "retention.ms",
					ConfigValue: "259200000", // 3 days
				},
			},
		},
		{
			Topic:             "kpi-errors-per-minute",
			NumPartitions:     1,
			ReplicationFactor: 1,
			ConfigEntries: []kafka.ConfigEntry{
				{
					ConfigName:  "retention.ms",
					ConfigValue: "604800000", // 7 days
				},
			},
		},
		{
			Topic:             "kpi-processing-latency",
			NumPartitions:     1,
			ReplicationFactor: 1,
			ConfigEntries: []kafka.ConfigEntry{
				{
					ConfigName:  "retention.ms",
					ConfigValue: "604800000", // 7 days
				},
			},
		},
	}

	// Create Kafka admin client with retries
	var conn *kafka.Conn
	var err error
	maxRetries := 5
	retryDelay := time.Second * 2

	for i := 0; i < maxRetries; i++ {
		conn, err = kafka.Dial("tcp", brokerAddress)
		if err == nil {
			defer conn.Close()
			break
		}
		
		log.Printf("Attempt %d: Failed to connect to Kafka broker: %v. Retrying in %v...", i+1, err, retryDelay)
		
		if i < maxRetries-1 {
			time.Sleep(retryDelay)
			// Increase delay for next retry
			retryDelay *= 2
		}
	}
	
	if err != nil {
		log.Printf("Failed to connect to Kafka broker after %d attempts: %v", maxRetries, err)
		// Don't return here, we'll try to create topics anyway in case the connection issue is temporary
	}

	// List existing topics first
	existingTopics, err := conn.ReadPartitions()
	if err != nil {
		log.Printf("Failed to read existing topics: %v", err)
	} else {
		existingTopicMap := make(map[string]bool)
		for _, partition := range existingTopics {
			existingTopicMap[partition.Topic] = true
		}

		log.Printf("Existing topics: %v", existingTopicMap)
	}

	// Create topics with retry logic for each topic
	for i, topic := range topics {
		// Create a fresh connection for each topic to avoid EOF errors
		if i > 0 {
			// Close previous connection if it exists
			if conn != nil {
				conn.Close()
			}
			
			// Create a new connection
			var connErr error
			for retry := 0; retry < 3; retry++ {
				conn, connErr = kafka.Dial("tcp", brokerAddress)
				if connErr == nil {
					break
				}
				log.Printf("Reconnection attempt %d: %v", retry+1, connErr)
				time.Sleep(time.Second)
			}
			
			if connErr != nil {
				log.Printf("Failed to reconnect to Kafka for topic %s: %v", topic.Topic, connErr)
				continue // Skip this topic and try the next one
			}
		}
		
		var topicErr error
		for attempt := 0; attempt < 3; attempt++ {
			topicErr = conn.CreateTopics(topic)
			if topicErr == nil {
				log.Printf("Topic %s created successfully", topic.Topic)
				break
			} else if topicErr.Error() == "kafka server: Topic already exists." {
				log.Printf("Topic %s already exists", topic.Topic)
				break
			} else if topicErr.Error() == "EOF" || 
				topicErr.Error() == "write tcp: use of closed network connection" ||
				topicErr.Error() == "write tcp: broken pipe" {
				log.Printf("Connection error when creating topic %s: %v. Reconnecting...", topic.Topic, topicErr)
				
				// Reconnect and try again
				if conn != nil {
					conn.Close()
				}
				
				var reconnErr error
				conn, reconnErr = kafka.Dial("tcp", brokerAddress)
				if reconnErr != nil {
					log.Printf("Failed to reconnect to Kafka: %v", reconnErr)
					time.Sleep(time.Second * 2)
					continue
				}
			} else {
				log.Printf("Attempt %d: Failed to create topic %s: %v", attempt+1, topic.Topic, topicErr)
				if attempt < 2 { // Only sleep if we're going to retry
					time.Sleep(time.Second * 2)
				}
			}
		}
		
		if topicErr != nil && topicErr.Error() != "kafka server: Topic already exists." {
			log.Printf("Warning: Could not create topic %s after multiple attempts: %v", topic.Topic, topicErr)
		}
	}

	// Verify topics exist after creation
	var topicsAfterCreation []kafka.Partition
	topicsAfterCreation, err = conn.ReadPartitions()
	if err != nil {
		log.Printf("Failed to read topics after creation: %v", err)
	} else {
		existingTopicMap := make(map[string]bool)
		for _, partition := range topicsAfterCreation {
			existingTopicMap[partition.Topic] = true
		}

		log.Printf("Topics after creation: %v", existingTopicMap)

		// Check if all required topics exist
		for _, topic := range topics {
			if _, exists := existingTopicMap[topic.Topic]; !exists {
				log.Printf("WARNING: Topic %s still does not exist after creation attempt", topic.Topic)
			}
		}
	}
}