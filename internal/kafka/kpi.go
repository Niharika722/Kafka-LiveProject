package kafka

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	errorCount     uint64       // Atomic counter for errors
	lastErrorReset time.Time    // Time of last error count reset
	errorMutex     sync.Mutex   // Mutex for error count operations
)

// trackErrorsPerMinute periodically calculates and publishes the errors-per-minute KPI
func trackErrorsPerMinute(brokerAddress string) {

	// Create a writer for the KPI topic
	kpiWriter := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    "kpi-errors-per-minute",
		Balancer: &kafka.LeastBytes{},
	}
	defer kpiWriter.Close()
	
	// Publish KPI every minute
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	
	for range ticker.C {
		errorMutex.Lock()
		
		// Get the current error count
		currentErrorCount := atomic.LoadUint64(&errorCount)
		
		// Calculate time elapsed since last reset
		elapsed := time.Since(lastErrorReset).Minutes()
		
		// Calculate errors per minute
		errorsPerMinute := float64(currentErrorCount) / elapsed
		
		// Reset the counter
		atomic.StoreUint64(&errorCount, 0)
		lastErrorReset = time.Now()
		
		errorMutex.Unlock()
		
		// Publish the KPI
		metadata := map[string]interface{}{
			"totalErrors": currentErrorCount,
			"timeSpanMinutes": elapsed,
		}
		
		PublishKPIEvent(kpiWriter, "InventoryConsumerErrors", "ErrorsPerMinute", errorsPerMinute, "InventoryConsumer", metadata)
	}
}



var latencyStats = struct {
	count       int
	totalMs     float64
	maxMs       float64
	lastReported time.Time
	mutex       sync.Mutex
}{lastReported: time.Now()}

// recordLatency records the processing latency for KPI tracking
func recordLatency(latencyMs float64) {
	latencyStats.mutex.Lock()
	defer latencyStats.mutex.Unlock()
	
	latencyStats.count++
	latencyStats.totalMs += latencyMs
	latencyStats.maxMs = math.Max(latencyStats.maxMs, latencyMs)
}

// trackProcessingLatency periodically calculates and publishes the processing latency KPI
func trackProcessingLatency(brokerAddress string) {
	// Create a writer for the KPI topic
	kpiWriter := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    "kpi-processing-latency",
		Balancer: &kafka.LeastBytes{},
	}
	defer kpiWriter.Close()
	
	// Publish KPI every minute
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	
	for range ticker.C {
		latencyStats.mutex.Lock()
		
		// Skip if no orders were processed
		if latencyStats.count == 0 {
			latencyStats.mutex.Unlock()
			continue
		}
		
		// Calculate average latency
		avgLatencyMs := latencyStats.totalMs / float64(latencyStats.count)
		maxLatencyMs := latencyStats.maxMs
		count := latencyStats.count
		
		// Reset stats
		latencyStats.count = 0
		latencyStats.totalMs = 0
		latencyStats.maxMs = 0
		latencyStats.lastReported = time.Now()
		
		latencyStats.mutex.Unlock()
		
		// Publish average latency KPI
		metadata := map[string]interface{}{
			"ordersProcessed": count,
			"maxLatencyMs": maxLatencyMs,
		}
		
		// Publish average latency KPI
		PublishKPIEvent(kpiWriter, "WarehouseProcessingLatency", "AvgLatencyMs", avgLatencyMs, "WarehouseConsumer", metadata)
		
		// Publish max latency KPI
		PublishKPIEvent(kpiWriter, "WarehouseProcessingLatency", "MaxLatencyMs", maxLatencyMs, "WarehouseConsumer", metadata)
	}
}
