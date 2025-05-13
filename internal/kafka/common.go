package kafka

type OrderEvent struct {
	EventType string `json:"eventType"`
	Timestamp string `json:"timestamp"`
	OrderID   string `json:"orderId"`
}

type OrderConfirmedEvent struct {
	EventType        string      `json:"eventType"`
	Timestamp        string      `json:"timestamp"`
	OrderID          string      `json:"orderId"`
	Status           string      `json:"status"`
	ConfirmationDate string      `json:"confirmationDate,omitempty"`
	IsDuplicate      bool        `json:"isDuplicate,omitempty"`
	OriginalEvent    OrderEvent  `json:"originalEvent,omitempty"`
	ProcessingTime   string      `json:"processingTime,omitempty"`
}

type NotificationEvent struct {
	EventType string `json:"eventType"`
	Timestamp string `json:"timestamp"`
	OrderID   string `json:"orderId"`
	Message   string `json:"message"`
}

type WarehouseEvent struct {
	EventType string `json:"eventType"`
	Timestamp string `json:"timestamp"`
	OrderID   string `json:"orderId"`
	Status    string `json:"status"`
	// Fields for more complex events
	ConfirmationDate string                 `json:"confirmationDate,omitempty"`
	IsDuplicate      bool                   `json:"isDuplicate,omitempty"`
	OriginalEvent    map[string]interface{} `json:"originalEvent,omitempty"`
	ProcessingTime   string                 `json:"processingTime,omitempty"`
}

type ShipperEvent struct {
	EventType    string `json:"eventType"`
	Timestamp    string `json:"timestamp"`
	OrderID      string `json:"orderId"`
	CustomerID   string `json:"customerId"`
	ShippingInfo struct {
		Address     string `json:"address"`
		City        string `json:"city"`
		PostalCode  string `json:"postalCode"`
		TrackingNum string `json:"trackingNum"`
	} `json:"shippingInfo"`
}

type ErrorEvent struct {
	EventType       string `json:"eventType"`
	Timestamp       string `json:"timestamp"`
	Error           string `json:"error"`
	OriginalMessage string `json:"originalMessage"`
}

// KPIEvent represents a key performance indicator event
type KPIEvent struct {
	EventType   string      `json:"eventType"`
	Timestamp   string      `json:"timestamp"`
	KPIName     string      `json:"kpiName"`
	MetricName  string      `json:"metricName"`
	MetricValue float64     `json:"metricValue"`
	ServiceName string      `json:"serviceName"`
	Metadata    interface{} `json:"metadata,omitempty"`
}

// // PublishKPIEvent publishes a KPI event to the specified Kafka topic
// func PublishKPIEvent(writer *kafka.Writer, kpiName, metricName string, metricValue float64, serviceName string, metadata interface{}) error {
// 	// Create KPI event
// 	kpiEvent := KPIEvent{
// 		EventType:   "KPIEvent",
// 		Timestamp:   time.Now().Format(time.RFC3339),
// 		KPIName:     kpiName,
// 		MetricName:  metricName,
// 		MetricValue: metricValue,
// 		ServiceName: serviceName,
// 		Metadata:    metadata,
// 	}
	
// 	// Marshal the KPI event
// 	kpiJSON, err := json.Marshal(kpiEvent)
// 	if err != nil {
// 		log.Printf("Error marshalling KPI event: %v", err)
// 		return err
// 	}
	
// 	// Generate a unique key for the message
// 	kpiKey := fmt.Sprintf("kpi-%s-%s-%d", kpiName, metricName, time.Now().UnixNano())
	
// 	// Publish to KPI topic
// 	log.Printf("Publishing KPI event: %s - %s = %f", kpiName, metricName, metricValue)
// 	if err := writer.WriteMessages(context.Background(), kafka.Message{
// 		Key:   []byte(kpiKey),
// 		Value: kpiJSON,
// 	}); err != nil {
// 		log.Printf("Error publishing KPI event: %v", err)
// 		return err
// 	}
	
// 	log.Printf("Successfully published KPI event with key %s", kpiKey)
// 	return nil
// }

// // Helper function to publish error events to the DeadLetterQueue topic
// func publishErrorEvent(errorWriter *kafka.Writer, errorMessage, originalMessage string) {
// 	// Create error event
// 	errorEvent := ErrorEvent{
// 		EventType:       "ProcessingError",
// 		Timestamp:       time.Now().Format(time.RFC3339),
// 		Error:           errorMessage,
// 		OriginalMessage: originalMessage,
// 	}
	
// 	// Marshal the error event
// 	errorJSON, err := json.Marshal(errorEvent)
// 	if err != nil {
// 		log.Printf("Error marshalling error event: %v", err)
// 		return
// 	}
	
// 	// Generate a unique key for the message
// 	errorKey := fmt.Sprintf("error-%d", time.Now().UnixNano())
	
// 	// Publish to error-events topic
// 	log.Printf("Publishing error event to topic: error-events - %s", errorMessage)
// 	if err := errorWriter.WriteMessages(context.Background(), kafka.Message{
// 		Key:   []byte(errorKey),
// 		Value: errorJSON,
// 	}); err != nil {
// 		log.Printf("Error publishing to error-events topic: %v", err)
// 		return
// 	}
	
// 	log.Printf("Successfully published error event to topic error-events with key %s", errorKey)
// }

// cleanMessageValue removes any trailing control characters from the message
func CleanMessageValue(value []byte) []byte {
	// Find the last valid JSON character (closing brace)
	len := len(value)
	for i := len - 1; i >= 0; i-- {
		if value[i] == '}' {
			return value[:i+1]
		}
	}
	return value
}
