package health

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

func KafkaHealthCheck(ctx *gin.Context) {
	// Use our internal kafka package to check health
	err := checkKafkaConnection("localhost:9092")
	if err != nil {
		ctx.JSON(500, gin.H{"status": "error", "message": fmt.Sprintf("Failed to connect to Kafka: %v", err)})
		return  
	}
	
	ctx.JSON(200, gin.H{"status": "success", "message": "Kafka connection successful"})
}

// CheckKafkaConnection checks if a connection to Kafka can be established
func checkKafkaConnection(brokerAddress string) error {
	// Try to connect to Kafka
	conn, err := kafka.Dial("tcp", brokerAddress)
	if err != nil {
		return err
	}
	defer conn.Close()
	
	return nil
}