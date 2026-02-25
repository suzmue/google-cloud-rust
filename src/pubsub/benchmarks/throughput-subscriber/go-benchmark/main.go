// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"google.golang.org/api/option"
)

var (
	recvCount  atomic.Int64
	recvBytes  atomic.Int64
	errorCount atomic.Int64
)

func main() {
	project := flag.String("project", "", "The Google Cloud project ID.")
	subscriptionName := flag.String("subscription-id", "", "The Pub/Sub subscription ID.")
	iterationDuration := flag.Duration("iteration-duration", 5*time.Second, "The duration of each iteration.")
	maximumRuntime := flag.Duration("maximum-runtime", 1*time.Minute, "The maximum runtime of the benchmark.")
	maxOutstandingMessages := flag.Int("max-outstanding-messages", 100000, "The maximum number of outstanding messages.")
	subscriberIOChannels := flag.Int("subscriber-io-channels", 1, "The number of subscriber I/O channels.")
	subscriberThreadCount := flag.Int("subscriber-thread-count", 1, "The number of subscriber threads.")
	flag.Parse()

	if *project == "" {
		log.Fatal("-project is required")
	}
	if *subscriptionName == "" {
		log.Fatal("-subscription-id is required")
	}

	fmt.Printf("# Running Cloud Pub/Sub subscriber benchmark with config: {project:%s subscription:%s iteration-duration:%s maximum-runtime:%s max-outstanding-messages:%d subscriber-io-channels:%d subscriber-thread-count:%d}\n", *project, *subscriptionName, *iterationDuration, *maximumRuntime, *maxOutstandingMessages, *subscriberIOChannels, *subscriberThreadCount)
	fmt.Println("timestamp,elapsed(s),op,iteration,count,msgs/s,bytes,MB/s,errors,errors/s")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := pubsub.NewClient(ctx, *project, option.WithGRPCConnectionPool(*subscriberIOChannels))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	sub := client.Subscriber(fmt.Sprintf("projects/%s/subscriptions/%s", *project, *subscriptionName))
	sub.ReceiveSettings.MaxOutstandingMessages = *maxOutstandingMessages
	sub.ReceiveSettings.NumGoroutines = *subscriberThreadCount
	sub.ReceiveSettings.MaxOutstandingBytes = 1_000_000_000 // 1 GB

	go func() {
		err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			recvCount.Add(1)
			recvBytes.Add(int64(len(msg.Data)))
			msg.Ack()
		})
		if err != nil && err != context.Canceled {
			log.Printf("Receive error: %v", err)
			errorCount.Add(1)
		}
	}()

	startTime := time.Now()
	for i := int64(0); ; i++ {
		if done(*maximumRuntime, i, startTime) {
			break
		}

		timer := time.Now()
		startRecvCount := recvCount.Load()
		startRecvBytes := recvBytes.Load()
		startErrorCount := errorCount.Load()

		time.Sleep(*iterationDuration)

		recvCountLast := recvCount.Load() - startRecvCount
		recvBytesLast := recvBytes.Load() - startRecvBytes
		errorCountLast := errorCount.Load() - startErrorCount
		usage := time.Since(timer)

		printResult("Recv", i, recvCountLast, recvBytesLast, errorCountLast, usage)
	}

	fmt.Printf("# Subscriber: recv_count=%d, error_count=%d\n", recvCount.Load(), errorCount.Load())
}

func done(maximumRuntime time.Duration, samples int64, start time.Time) bool {
	now := time.Now()
	return now.After(start.Add(maximumRuntime))
}

func printResult(operation string, iteration int64, count int64, bytes int64, errors int64, elapsed time.Duration) {
	elapsedS := elapsed.Seconds()
	mbs := float64(bytes) / elapsedS / 1_000_000.0
	msgs := float64(count) / elapsedS
	errs := float64(errors) / elapsedS
	fmt.Printf("%d,%.2f,%s,%d,%d,%.2f,%d,%.2f,%d,%.2f\n",
		time.Now().UnixMilli(),
		elapsedS,
		operation,
		iteration,
		count,
		msgs,
		bytes,
		mbs,
		errors,
		errs,
	)
}
