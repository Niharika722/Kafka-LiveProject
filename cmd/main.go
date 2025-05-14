package main

import (
	"log"
	"order-service/internal/config"
	"order-service/internal/health"
	"order-service/internal/kafka"

	"github.com/gin-gonic/gin"
)

func main() {

	router := gin.Default()

	// Kafka broker address - ensure it's accessible 
	brokerAddress := "localhost:9092"
	
	// Log startup information
	log.Printf("Starting order service with Kafka broker at %s", brokerAddress)

	// Set up Kafka topics
	log.Println("Setting up Kafka topics...")
	config.SetupTopics(brokerAddress)

	log.Println("Kafka topics setup completed")

	// Endpoint to create an order
	router.POST("/order", kafka.CreateOrder)
	
	// Kafka health check endpoint
	router.GET("/kafka-health", health.KafkaHealthCheck)

	log.Println("Starting direct stream processor...")

	// Start the direct stream processor in a goroutine
	go func() {		
		log.Println("Inventory consumer started")
		kafka.StartInventoryConsumer(brokerAddress, "order-received", "order-confirmed", "error-events")
	}()

	// Start the warehouse consumer for the "notification" topic
	go func() {
		log.Println("Warehouse consumer started")
		kafka.StartWarehouseConsumer(
			brokerAddress, // Kafka broker address
			"order-confirmed",   // Order confirmed topic
			"order-picked-and-packed",   // Order picked and packed topic
			"notification",   // Notification topic
			"error-events",   // DeadLetterQueue topic
		)
	}()

	// Start the Notification consumer for the "notification" topic
	go func() {
		log.Println("Notification consumer started")
		kafka.StartNotificationConsumer(
			brokerAddress, // Kafka broker address
			"notification",   // Notification topic
			"error-events",   // DeadLetterQueue topic
		)
	}()

	// Start the Shipper consumer for the "order-picked-and-packed" topic
	go func() {
		log.Println("Shipper consumer started")
		kafka.StartShipperConsumer(
			brokerAddress,        // Kafka broker address
			"order-picked-and-packed", // Order picked and packed topic
			"notification",         // Notification topic
			"error-events",         // DeadLetterQueue topic
		)
	}()

	// Start the HTTP server
	router.Run(":9090")
}
