package main

import (
	"fmt"
	"github.com/slinkydeveloper/loadastic/common"
	"github.com/slinkydeveloper/loadastic/kafka"
	"os"
	"time"
)

func main() {
	sender, err := kafka.NewKafkaSender(os.Getenv("BOOTSTRAP_URL"), os.Getenv("TOPIC"))
	if err != nil {
		panic(err)
	}

	okCount := 0
	failedCount := 0

	loadastic := kafka.NewLoadastic(
		sender,
		kafka.WithAfterSend(func(request kafka.RecordPayload, response interface{}, id uint64, uuid string) {
			okCount++
		}),
		kafka.WithAfterFailed(func(request kafka.RecordPayload, err error, id uint64, uuid string) {
			failedCount++
		}),
	)

	okCount = 0
	failedCount = 0

	loadastic.StartSteps(kafka.RandomRequestFactory(100), common.Step{Rps: 100, Duration: 10 * time.Second})

	fmt.Printf("Ok count: %d, Failed count: %d\n", okCount, failedCount)

	okCount = 0
	failedCount = 0

	loadastic.StartSteps(kafka.RandomRequestFactory(100), common.Step{Rps: 1000, Duration: 10 * time.Second})

	fmt.Printf("Ok count: %d, Failed count: %d\n", okCount, failedCount)

	okCount = 0
	failedCount = 0

	loadastic.StartSteps(kafka.RandomRequestFactory(100), common.Step{Rps: 10000, Duration: 10 * time.Second})

	fmt.Printf("Ok count: %d, Failed count: %d\n", okCount, failedCount)
}
