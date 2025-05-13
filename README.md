# Kafka Microservices Project

## Overview
This project demonstrates a microservices architecture using Apache Kafka for event-driven communication between services. The system simulates an order processing workflow with multiple stages including inventory verification, warehouse processing, shipping, and customer notifications.

## Project Structure
```
microservices/
└── order-service/
    ├── cmd/
    │   └── main.go         # Application entry point
    ├── internal/
    │   ├── config/         # Configuration management
    │   ├── health/         # Health check endpoints
    │   └── kafka/          # Kafka producers and consumers
    ├── docker-compose.yml  # Docker configuration for Kafka
    ├── go.mod              # Go module dependencies
    └── go.sum              # Go module checksums
```

## Services and Workflow
The application implements an order processing workflow with the following components:

1. **Order Service**: Receives order requests and initiates the order processing workflow
2. **Inventory Consumer**: Verifies inventory availability for orders
3. **Warehouse Consumer**: Handles order picking and packing
4. **Shipper Consumer**: Manages order shipping
5. **Notification Consumer**: Sends notifications to customers

## Kafka Topics
The system uses the following Kafka topics for communication:
- `order-received`: New orders received
- `order-confirmed`: Orders confirmed after inventory check
- `order-picked-and-packed`: Orders that have been picked and packed
- `notification`: Customer notifications
- `error-events`: Error handling and dead letter queue

## Getting Started

### Prerequisites
- Go 1.24 or higher
- Docker and Docker Compose

### Setup
1. Clone the repository
2. Navigate to the project directory
3. Create a `.env` file with the following variables:
   ```
   KAFKA_HOME=your_kafka_home_path
   BROKER_ADDRESS=localhost:9092
   NOTIFICATION_TOPIC=notification
   ```

### Running the Application
1. Start Kafka using Docker Compose:
   ```
   cd microservices/order-service
   docker-compose up -d
   ```

2. Run the order service:
   ```
   cd microservices/order-service
   go run cmd/main.go
   ```

3. The service will be available at `http://localhost:9090`

## API Endpoints
- `POST /order`: Create a new order
- `GET /kafka-health`: Check Kafka connection health

## Development
This project uses Go modules for dependency management. To add new dependencies:
```
go get github.com/example/package
```

## License
APACHE-2.0
# Kafka-LiveProject
# Kafka-LiveProject
