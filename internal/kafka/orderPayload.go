package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"github.com/gin-gonic/gin"
)
type Payload struct {
	OrderID    string  `json:"orderId"`
	CustomerID string  `json:"customerId"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
}

var payloads []Payload

func CreateOrder(ctx *gin.Context) {

	var newPayload Payload
	if err := ctx.ShouldBindJSON(&newPayload); err != nil {
		ctx.JSON(400, gin.H{"status": "error", "message": "Invalid payload"})
		return
	}

	// Validate the payload
	if err := validateOrderPayload(newPayload); err != nil {
		ctx.JSON(400, gin.H{"status": "error", "message": err.Error()})
		return
	}

	payloads = append(payloads, newPayload)

	// Translate the payload into the event schema
	eventMessage, err := translateToEventSchema(newPayload)
	if err != nil {
		ctx.JSON(500, gin.H{"status": "error", "message": "Failed to translate payload"})
		return
	}

	// Publish the event to Kafka
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = PublishEvent(c, eventMessage, "localhost:9092", "order-received")
	if err != nil {
		ctx.JSON(500, gin.H{"status": "error", "message": "Failed to publish event"})
		return
	}

	ctx.JSON(200, gin.H{"status": "success", "message": "Order received and event published"})
}

func validateOrderPayload(p Payload) error {
	if p.OrderID == "" {
		return fmt.Errorf("OrderID is required")
	}
	return nil
}

func translateToEventSchema(p Payload) (string, error) {
	event := struct {
		EventType string `json:"eventType"`
		Timestamp string `json:"timestamp"`
		OrderID   string `json:"orderId"`
	}{
		EventType: "OrderReceived",
		Timestamp: time.Now().Format(time.RFC3339),
		OrderID:   p.OrderID,
	}

	eventJSON, err := json.Marshal(event)
	if err != nil {
		return "", err
	}

	return string(eventJSON), nil
}