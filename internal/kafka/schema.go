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

type KPIEvent struct {
	EventType   string      `json:"eventType"`
	Timestamp   string      `json:"timestamp"`
	KPIName     string      `json:"kpiName"`
	MetricName  string      `json:"metricName"`
	MetricValue float64     `json:"metricValue"`
	ServiceName string      `json:"serviceName"`
	Metadata    interface{} `json:"metadata,omitempty"`
}
