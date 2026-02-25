package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"golang.org/x/sync/semaphore"
	"google.golang.org/api/option"
)

type Config struct {
	Project                 string
	TopicID                 string
	PayloadSize             int64
	IterationDuration       time.Duration
	PublisherMaxBatchSize   int
	PublisherMaxBatchBytes  int
	MaximumRuntime          time.Duration
	PublisherIOChannels     int
	MaxOutstandingMessages int64
}

var (
	sendCount  atomic.Int64
	sendBytes  atomic.Int64
	ackCount   atomic.Int64
	ackBytes   atomic.Int64
	errorCount atomic.Int64
)

func main() {
	config := Config{}
	flag.StringVar(&config.Project, "project", "", "Google Cloud Project ID")
	flag.StringVar(&config.TopicID, "topic-id", "", "Pub/Sub Topic ID")
	flag.Int64Var(&config.PayloadSize, "payload-size", 1024, "Payload size in bytes")
	flag.DurationVar(&config.IterationDuration, "iteration-duration", 5*time.Second, "Iteration duration")
	flag.IntVar(&config.PublisherMaxBatchSize, "publisher-max-batch-size", 1000, "Max batch size in messages")
	flag.IntVar(&config.PublisherMaxBatchBytes, "publisher-max-batch-bytes", 10*1024*1024, "Max batch size in bytes")
	flag.DurationVar(&config.MaximumRuntime, "maximum-runtime", 5*time.Minute, "Maximum runtime")
	flag.IntVar(&config.PublisherIOChannels, "publisher-io-channels", 1, "Number of gRPC channels")
	flag.Int64Var(&config.MaxOutstandingMessages, "max-outstanding-messages", 100000, "Maximum number of outstanding messages")
	flag.Parse()

	if config.Project == "" {
		fmt.Fprintln(os.Stderr, "Missing required flag: -project")
		flag.Usage()
		os.Exit(1)
	}

	fmt.Printf("# Running Cloud Pub/Sub benchmark with config: %+v\n", config)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	client, err := pubsub.NewClient(ctx, config.Project, option.WithGRPCConnectionPool(config.PublisherIOChannels))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	if config.TopicID == "" {
		log.Fatal("Topic ID is required (auto-creation not implemented)")
	}

	publisher := client.Publisher(config.TopicID)
	publisher.PublishSettings.CountThreshold = config.PublisherMaxBatchSize
	publisher.PublishSettings.ByteThreshold = config.PublisherMaxBatchBytes
	publisher.PublishSettings.DelayThreshold = config.MaximumRuntime

	payload := make([]byte, config.PayloadSize)

	var wg sync.WaitGroup
	sem := semaphore.NewWeighted(config.MaxOutstandingMessages)

	// Publisher loop
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := sem.Acquire(ctx, 1); err != nil {
					return
				}
				msg := &pubsub.Message{
					Data: payload,
				}
				res := publisher.Publish(ctx, msg)
				sendCount.Add(1)
				sendBytes.Add(config.PayloadSize)

				wg.Add(1)
				go func() {
					defer wg.Done()
					defer sem.Release(1)
					_, err := res.Get(ctx)
					if err != nil {
						if err != context.Canceled {
							errorCount.Add(1)
						}
					} else {
						ackCount.Add(1)
						ackBytes.Add(config.PayloadSize)
					}
				}()
			}
		}
	}()

	// Reporting loop
	startTime := time.Now()
	fmt.Println("timestamp,elapsed(s),op,iteration,count,msgs/s,bytes,MB/s")

	for i := int64(0); ; i++ {
		if done(&config, i, startTime) {
			break
		}

		timer := time.Now()
		startSendCount := sendCount.Load()
		startSendBytes := sendBytes.Load()
		startAckCount := ackCount.Load()
		startAckBytes := ackBytes.Load()

		time.Sleep(config.IterationDuration)

		sendCountLast := sendCount.Load() - startSendCount
		sendBytesLast := sendBytes.Load() - startSendBytes
		ackCountLast := ackCount.Load() - startAckCount
		ackBytesLast := ackBytes.Load() - startAckBytes
		usage := time.Since(timer)

		printResult("Pub", i, sendCountLast, sendBytesLast, usage)
		printResult("Ack", i, ackCountLast, ackBytesLast, usage)
	}

	publisher.Stop()
	wg.Wait()

	fmt.Printf("# Publisher: error_count=%d, ack_count=%d, send_count=%d\n",
		errorCount.Load(), ackCount.Load(), sendCount.Load())
}

func done(config *Config, samples int64, start time.Time) bool {
	now := time.Now()
	return now.After(start.Add(config.MaximumRuntime))
}

func printResult(operation string, iteration int64, count int64, bytes int64, elapsed time.Duration) {
	elapsedS := elapsed.Seconds()
	mbs := float64(bytes) / elapsedS / 1_000_000.0
	msgs := float64(count) / elapsedS
	fmt.Printf("%d,%g,%s,%d,%d,%.2f,%d,%.2f\n",
		time.Now().UnixMilli(),
		elapsedS,
		operation,
		iteration,
		count,
		msgs,
		bytes,
		mbs,
	)
}
