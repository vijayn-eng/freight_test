package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	// MESSAGE_SIZE_BYTES defines the size of each random message (100 KB)
	MESSAGE_SIZE_BYTES = 100 * 1024
	// REPORT_INTERVAL_SECONDS defines how often to print progress updates
	REPORT_INTERVAL_SECONDS = 5
)

// Global counters for tracking messages
var (
	messagesProduced int64
	messagesFailed   int64
	// Mutex to protect access to global counters from multiple goroutines
	counterMutex sync.Mutex
)

// generateRandomString generates a random string of a given length.
// For high performance, consider generating random bytes directly if the content doesn't need to be human-readable.
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()_+-=[]{}|;':,.<>/?`~"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// deliveryReportHandler listens for delivery reports from a Kafka producer.
// It updates global counters and signals the WaitGroup for each message.
func deliveryReportHandler(p *kafka.Producer, wg *sync.WaitGroup, threadID int) {
	for e := range p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			// A message was delivered or failed to be delivered
			if ev.TopicPartition.Error != nil {
				// Uncomment for verbose per-message error logging:
				// log.Printf("Thread %d: Delivery failed for message to topic %s [%d] at offset %v: %v\n",
				// 	threadID, *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset, ev.TopicPartition.Error)
				counterMutex.Lock()
				messagesFailed++
				counterMutex.Unlock()
			} else {
				// Uncomment for verbose per-message success logging:
				// log.Printf("Thread %d: Delivered message to topic %s [%d] at offset %v\n",
				// 	threadID, *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				counterMutex.Lock()
				messagesProduced++
				counterMutex.Unlock()
			}
			wg.Done() // Signal that this message's lifecycle is complete
		case kafka.Error:
			// General producer error (e.g., connection issues)
			log.Printf("Thread %d: Producer error: %v\n", threadID, ev)
		default:
			// Other events, usually ignored for basic producers
			// fmt.Printf("Thread %d: Ignored event: %v\n", threadID, ev)
		}
	}
}

// producerWorker is the goroutine that handles message production.
// Each worker gets its own Kafka producer instance.
func producerWorker(
	broker string,
	apiKey string,
	apiSecret string,
	topic string,
	numMessages int64,
	wg *sync.WaitGroup,
	threadID int,
) {
	// Ensure this worker's WaitGroup counter is decremented when it exits
	defer wg.Done()

	// Create a new Kafka producer instance for this worker
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"sasl.username":     apiKey,
		"sasl.password":     apiSecret,
		"client.id":         fmt.Sprintf("go-producer-thread-%d", threadID), // Unique client ID per producer
		"acks":              "all", // Ensure all replicas acknowledge the message (strong durability)
		"linger.ms":         100,    // Batch messages for better throughput (10ms wait for more messages)
		"batch.size":        10 * 1024 * 1024, // 5 MB batch size (adjust based on message size and network)
		//"compression.type":  "lz4", // LZ4 compression (good balance of speed and ratio)
		"queue.buffering.max.messages": 100000, // Max messages in producer queue
		"queue.buffering.max.kbytes":   1024 * 1024, // 1 GB max buffer size
		"message.timeout.ms":           300000, // 5 minutes message timeout (time to deliver message)
		"enable.idempotence": "false",
		"max.in.flight.requests.per.connection": 5,
	})
	if err != nil {
		log.Printf("Thread %d: Failed to create producer: %v\n", threadID, err)
		// If producer creation fails, we can't produce any messages for this worker
		// So we decrement the overall messagesWg for the messages this worker was supposed to send
		counterMutex.Lock()
		messagesFailed += numMessages // Assume all messages for this worker failed
		counterMutex.Unlock()
		return
	}
	defer producer.Close() // Ensure producer is closed when the goroutine exits

	log.Printf("Thread %d: Starting to produce %d messages to topic '%s'...\n", threadID, numMessages, topic)

	// WaitGroup to track messages produced by THIS worker, so deliveryReportHandler can signal completion
	var workerMessagesWg sync.WaitGroup

	// Start a dedicated goroutine for this producer's delivery reports
	go deliveryReportHandler(producer, &workerMessagesWg, threadID)

	for i := int64(0); i < numMessages; i++ {
		messagePayload := generateRandomString(MESSAGE_SIZE_BYTES)
		messageKey := fmt.Sprintf("thread-%d-msg-%d", threadID, i) // Unique key for message

		workerMessagesWg.Add(1) // Increment for each message sent for this worker

		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(messageKey),
			Value:          []byte(messagePayload),
		}, nil) // Delivery report is handled by deliveryReportHandler

		if err != nil {
			// Handle local producer errors (e.g., queue full)
			if err.(kafka.Error).Code() == kafka.ErrQueueFull {
				log.Printf("Thread %d: Local producer queue full (%d messages awaiting delivery), flushing...\n", threadID, producer.Len())
				producer.Flush(15 * 1000) // Flush with a timeout of 15 seconds
				// After flush, retry producing the current message
				err = producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
					Key:            []byte(messageKey),
					Value:          []byte(messagePayload),
				}, nil)
				if err != nil {
					log.Printf("Thread %d: Failed to produce message even after flush: %v\n", threadID, err)
					counterMutex.Lock()
					messagesFailed++
					counterMutex.Unlock()
					workerMessagesWg.Done() // Signal this message as failed
				}
			} else {
				log.Printf("Thread %d: Failed to produce message: %v\n", threadID, err)
				counterMutex.Lock()
				messagesFailed++
				counterMutex.Unlock()
				workerMessagesWg.Done() // Signal this message as failed
			}
		}
	}

	// Flush any remaining messages in the producer's queue before this worker exits
	log.Printf("Thread %d: Flushing remaining messages...\n", threadID)
	producer.Flush(30 * 1000) // Flush with a generous timeout (30 seconds)

	// Wait for all messages sent by this worker to be delivered or fail
	workerMessagesWg.Wait()
	log.Printf("Thread %d: Finished producing all messages.\n", threadID)
}

func main() {
	// --- Command Line Argument Parsing ---
	if len(os.Args) < 6 {
		fmt.Printf("Usage: %s <broker_servers> <api_key> <api_secret> <topic> <num_messages> <num_producer_threads>\n", os.Args[0])
		os.Exit(1)
	}

	brokerServers := os.Args[1]
	apiKey := os.Args[2]
	apiSecret := os.Args[3]
	topic := os.Args[4]
	numMessages, err := strconv.ParseInt(os.Args[5], 10, 64)
	if err != nil {
		log.Fatalf("Invalid number of messages: %v", err)
	}
	numProducerThreads, err := strconv.Atoi(os.Args[6])
	if err != nil {
		log.Fatalf("Invalid number of producer threads: %v", err)
	}

	// --- Validate Confluent Cloud Configuration ---
	if strings.Contains(brokerServers, "YOUR_") || strings.Contains(apiKey, "YOUR_") || strings.Contains(apiSecret, "YOUR_") {
		log.Fatalf("ERROR: Please replace placeholder Confluent Cloud configurations in command line arguments.")
	}

	log.Println("--- Starting Go Multi-threaded Kafka Producer ---")
	log.Printf("Broker Servers: %s\n", brokerServers)
	log.Printf("Topic: %s\n", topic)
	log.Printf("Total messages to produce: %d\n", numMessages)
	log.Printf("Number of producer threads: %d\n", numProducerThreads)
	log.Printf("Message size: %d KB\n", MESSAGE_SIZE_BYTES/1024)
	log.Println(strings.Repeat("-", 40))

	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	// Calculate messages per thread
	messagesPerThread := numMessages / int64(numProducerThreads)
	remainingMessages := numMessages % int64(numProducerThreads)

	// WaitGroup to track completion of all producer worker goroutines
	var workerWg sync.WaitGroup

	startTime := time.Now()
	lastReportTime := startTime
	lastProducedCount := int64(0)

	// --- Start Producer Worker Goroutines ---
	for i := 0; i < numProducerThreads; i++ {
		threadMessages := messagesPerThread
		if int64(i) < remainingMessages {
			threadMessages++ // Distribute remaining messages evenly
		}
		if threadMessages == 0 { // Don't start a worker if it has no messages
			continue
		}

		workerWg.Add(1) // Increment WaitGroup for each worker goroutine
		go producerWorker(brokerServers, apiKey, apiSecret, topic, threadMessages, &workerWg, i+1)
	}

	// --- Monitor Progress ---
	// This goroutine periodically prints the production progress
	go func() {
		for {
			time.Sleep(REPORT_INTERVAL_SECONDS * time.Second)
			counterMutex.Lock()
			currentProduced := messagesProduced
			currentFailed := messagesFailed
			counterMutex.Unlock()

			totalProcessed := currentProduced + currentFailed
			if totalProcessed == 0 && time.Since(startTime) < REPORT_INTERVAL_SECONDS*time.Second {
				// Avoid reporting 0 progress too early
				continue
			}

			elapsedTotalTime := time.Since(startTime).Seconds()
			intervalMessages := currentProduced - lastProducedCount
			intervalTime := time.Since(lastReportTime).Seconds()

			var speedMsgPerSec float64
			var speedGBPerSec float64
			if intervalTime > 0 {
				speedMsgPerSec = float64(intervalMessages) / intervalTime
				speedGBPerSec = (speedMsgPerSec * float64(MESSAGE_SIZE_BYTES)) / (1024 * 1024 * 1024)
			}

			fmt.Printf("\rProgress: %d/%d (%.2f%%) | Produced: %d | Failed: %d | Throughput: %.2f GB/s | Total Time: %.2fs",
				totalProcessed, numMessages, float64(totalProcessed)/float64(numMessages)*100,
				currentProduced, currentFailed, speedGBPerSec, elapsedTotalTime)
			os.Stdout.Sync() // Ensure output is flushed immediately

			lastReportTime = time.Now()
			lastProducedCount = currentProduced

			if totalProcessed >= numMessages {
				break // All messages processed, exit monitoring
			}
		}
	}()

	// Wait for all producer worker goroutines to complete their work
	workerWg.Wait()

	endTime := time.Now()
	totalElapsedTime := endTime.Sub(startTime).Seconds()

	totalBytesProduced := messagesProduced * MESSAGE_SIZE_BYTES
	averageGBPerSec := (float64(totalBytesProduced) / (1024 * 1024 * 1024)) / totalElapsedTime

	log.Println("\n\n--- Production Summary ---")
	log.Printf("Total messages requested: %d\n", numMessages)
	log.Printf("Successfully produced: %d\n", messagesProduced)
	log.Printf("Failed to produce: %d\n", messagesFailed)
	log.Printf("Total data sent: %.2f GB\n", float64(totalBytesProduced)/(1024*1024*1024))
	log.Printf("Total time taken: %.2f seconds\n", totalElapsedTime)
	log.Printf("Average production rate: %.2f GB/second\n", averageGBPerSec)
	log.Println(strings.Repeat("-", 40))
}
