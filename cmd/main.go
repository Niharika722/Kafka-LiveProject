package main

import (
	"log"
	"order-service/internal/config"
	"order-service/internal/health"
	"order-service/internal/kafka"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

// func getPayloads(ctx *gin.Context) {
// 	if len(payloads) == 0 {
// 		ctx.JSON(404, gin.H{"status": "error", "message": "No payloads found"})
// 		return
// 	}
// 	ctx.JSON(200, gin.H{"status": "success", "payloads": payloads})
// }

// func sendEvent(ctx *gin.Context) {
// 	message := "Order event from GET request"

// 	// Create a context with a timeout
// 	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// 	defer cancel()

// 	// Publish the event to Kafka
// 	err := kafka.PublishEvent(c, message, "localhost:9092", "order-received")
// 	if err != nil {
// 		log.Println("Error publishing event:", err)
// 		ctx.JSON(500, gin.H{"status": "error", "message": err.Error()})
// 		return
// 	}

// 	ctx.JSON(200, gin.H{"status": "success", "message": "Kafka event sent"})
// }

func main() {

	// Load .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Use the environment variables
	kafkaHome := os.Getenv("KAFKA_HOME")
	broker := os.Getenv("BROKER_ADDRESS")
	notificationTopic := os.Getenv("NOTIFICATION_TOPIC")

	log.Println("Kafka Home:", kafkaHome)
	log.Println("Broker:", broker)
	log.Println("Notification Topic:", notificationTopic)
	
	router := gin.Default()

	// Kafka broker address - ensure it's accessible
	brokerAddress := "localhost:9092"
	
	// Log startup information
	log.Printf("Starting order service with Kafka broker at %s", brokerAddress)

	// Set up Kafka topics
	log.Println("Setting up Kafka topics...")
	config.SetupTopics(brokerAddress)
	
	// Explicitly create KPI topics
	log.Println("Setting up KPI topics...")
	// Uncomment if you have a separate function for KPI topics
	// kafka.CreateKPITopics(brokerAddress)
	log.Println("Kafka topics setup completed")
	
	// Health check endpoint
	// router.GET("/health", func(ctx *gin.Context) {
	// 	ctx.JSON(200, gin.H{"status": "order service is healthy"})
	// })

	// Endpoint to create an order
	router.POST("/order", kafka.CreateOrder)

	// Endpoint to trigger Kafka event
	// router.GET("/send", sendEvent)
	
	// Kafka health check endpoint
	router.GET("/kafka-health", health.KafkaHealthCheck)

	log.Println("Starting direct stream processor...")

	// Start the direct stream processor in a goroutine
	go func() {
		// Add a small delay to ensure the HTTP server is up first
		// time.Sleep(1 * time.Second)
		
		log.Println("Inventory consumer started")
		kafka.StartInventoryConsumer(brokerAddress, "order-received", "order-confirmed", "error-events")
		// if err != nil {
		// 	log.Printf("Error starting inventory consumer: %v", err)
		// }
	}()

	// Start the warehouse consumer for the "notification" topic
	go func() {
		// Add a small delay to ensure the HTTP server is up first
		// time.Sleep(1 * time.Second)

		log.Println("Warehouse consumer started")
		kafka.StartWarehouseConsumer(
			"localhost:9092", // Kafka broker address
			"order-confirmed",   // Order confirmed topic
			"order-picked-and-packed",   // Order picked and packed topic
			"notification",   // Notification topic
			"error-events",   // DeadLetterQueue topic
		)
	}()

	// Start the Notification consumer for the "notification" topic
	go func() {
		// Add a small delay to ensure the HTTP server is up first
		// time.Sleep(1 * time.Second)

		log.Println("Notification consumer started")
		kafka.StartNotificationConsumer(
			"localhost:9092", // Kafka broker address
			"notification",   // Notification topic
			"error-events",   // DeadLetterQueue topic
		)
	}()

	// Start the Shipper consumer for the "order-picked-and-packed" topic
	go func() {
		// Add a small delay to ensure the HTTP server is up first
		time.Sleep(1 * time.Second)

		log.Println("Shipper consumer started")
		kafka.StartShipperConsumer(
			"localhost:9092",        // Kafka broker address
			"order-picked-and-packed", // Order picked and packed topic
			"notification",         // Notification topic
			"error-events",         // DeadLetterQueue topic
		)
	}()

	// Start the HTTP server
	router.Run(":9090")
}
