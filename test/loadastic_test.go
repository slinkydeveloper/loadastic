package test

import (
	"fmt"
	"github.com/slinkydeveloper/loadastic/common"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestFailLogicAndHooksInvocation(t *testing.T) {
	tests := []struct {
		failRequest       bool
		failInFailChecker bool
	}{{
		failRequest:       false,
		failInFailChecker: false,
	},
		{
			failRequest:       true,
			failInFailChecker: false,
		},
		{
			failRequest:       true,
			failInFailChecker: true,
		}}
	for _, test := range tests {
		t.Run(fmt.Sprintf("fail: %v, fail in FailedChecker: %v", test.failRequest, test.failInFailChecker), func(t *testing.T) {
			beforeCount := 0
			afterSendCount := 0
			afterFailedCount := 0
			failureCheckCount := 0
			requestFactoryCount := 0
			sender := mockSender{
				count:    0,
				mustFail: test.failRequest && !test.failInFailChecker,
			}

			loadastic := NewLoadastic(&sender, WithBeforeSend(func(request mockRequest, tickerTimestamp time.Time, id uint64) {
				beforeCount++
			}), WithAfterSend(func(request mockRequest, response mockResponse, id uint64) {
				afterSendCount++
			}), WithAfterFailed(func(request mockRequest, err error, id uint64) {
				afterFailedCount++
			}), WithFailedChecker(func(response mockResponse) error {
				failureCheckCount++
				if test.failRequest && test.failInFailChecker {
					return fmt.Errorf("bla")
				}
				return nil
			}))

			loadastic.StartSteps(func(tickerTimestamp time.Time, id uint64) mockRequest {
				requestFactoryCount++
				return nil
			}, common.Step{Rps: 10, Duration: time.Second})

			assert.Equal(t, 10, beforeCount)
			if !test.failRequest {
				assert.Equal(t, 10, afterSendCount)
			} else {
				assert.Equal(t, 0, afterSendCount)
			}
			if test.failRequest {
				assert.Equal(t, 10, afterFailedCount)
			} else {
				assert.Equal(t, 0, afterFailedCount)
			}
			if test.failRequest && !test.failInFailChecker {
				assert.Equal(t, 0, failureCheckCount)
			} else {
				assert.Equal(t, 10, failureCheckCount)
			}

			assert.Equal(t, 10, sender.count)
			assert.Equal(t, 10, requestFactoryCount)
		})
	}
}

func TestWorkersScaleUp(t *testing.T) {
	sender := mockSender{count: 0, mustFail: false}
	afterSendCount := 0

	loadastic := NewLoadastic(&sender, WithAfterSend(func(request mockRequest, response mockResponse, id uint64) {
		afterSendCount++
	}))

	loadastic.StartSteps(func(tickerTimestamp time.Time, id uint64) mockRequest {
		return nil
	}, common.Step{Rps: 1000, Duration: time.Second})

	assert.Equal(t, 1000, sender.count)
	assert.Equal(t, 1000, afterSendCount)
}
