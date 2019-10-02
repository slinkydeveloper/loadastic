package templates

//go:generate genny -in=$GOFILE -out=../kafka/gen-$GOFILE -pkg=kafka gen "T_REQUEST=RecordPayload T_RESPONSE=interface{}"
//go:generate genny -in=$GOFILE -out=../test/gen-$GOFILE -pkg=test gen "T_REQUEST=mockRequest T_RESPONSE=mockResponse"

import (
	"github.com/cheekybits/genny/generic"
	"github.com/slinkydeveloper/loadastic/common"
	vegeta "github.com/tsenart/vegeta/lib"
	"runtime"
	"sync"
	"time"
)

type T_REQUEST generic.Type
type T_RESPONSE generic.Type

type BeforeSend func(request T_REQUEST, tickerTimestamp time.Time, id uint64)
type AfterSend func(request T_REQUEST, response T_RESPONSE, id uint64)
type AfterFailed func(request T_REQUEST, err error, id uint64)

type RequestFactory func(tickerTimestamp time.Time, id uint64) T_REQUEST
type FailedChecker func(response T_RESPONSE) error

type Sender interface {
	Send(request T_REQUEST) (T_RESPONSE, error)
}

type Loadastic struct {
	requestFactory RequestFactory
	failedChecker  FailedChecker
	sender Sender
	initialWorkers uint

	beforeSend BeforeSend
	afterSend AfterSend
	afterFailed AfterFailed
}

func NewLoadastic(requestFactory RequestFactory, sender Sender, opts ...func(Loadastic)) Loadastic {
	l := Loadastic{
		requestFactory: requestFactory,
		sender:   sender,
		initialWorkers: 10,

		failedChecker: func(response T_RESPONSE) error {
			return nil
		},
		beforeSend: func(request T_REQUEST, tickerTimestamp time.Time, id uint64) {},
		afterSend: func(request T_REQUEST, response T_RESPONSE, id uint64) {},
		afterFailed: func(request T_REQUEST, err error, id uint64) {},
	}
	
	for _, f := range opts {
		f(l)
	}
	
	return l
}

func WithFailedChecker(checker FailedChecker) func(Loadastic) {
	return func(loadastic Loadastic) {
		loadastic.failedChecker = checker
	}
}

func WithBeforeSend(beforeSend BeforeSend) func(Loadastic) {
	return func(loadastic Loadastic) {
		loadastic.beforeSend = beforeSend
	}
}

func WithAfterSend(afterSend AfterSend) func(Loadastic) {
	return func(loadastic Loadastic) {
		loadastic.afterSend = afterSend
	}
}

func WithAfterFailed(afterFailed AfterFailed) func(Loadastic) {
	return func(loadastic Loadastic) {
		loadastic.afterFailed = afterFailed
	}
}

func WithInitialWorkers(initialWorkers uint) func(Loadastic) {
	return func(loadastic Loadastic) {
		loadastic.initialWorkers = initialWorkers
	}
}

func (l Loadastic) StartSteps(steps ...common.Step) {
	for _, s := range steps {
		l.ExecutePace(vegeta.ConstantPacer{Freq: int(s.Rps), Per: time.Second}, s.Duration)
	}
}

func (l Loadastic) ExecutePace(pacer vegeta.Pacer, duration time.Duration) {
	workers := sync.WaitGroup{}
	jobsPool := sync.Pool{
		New: func() interface{} {
			return &common.Job{}
		},
	}
	jobsCh := make(chan *common.Job)

	for i := uint(0); i < l.initialWorkers; i++ {
		go l.worker(&workers, jobsCh, &jobsPool)
	}

	began, count := time.Now(), uint64(0)
	for {
		elapsed := time.Since(began)
		if duration > 0 && elapsed > duration {
			break
		}

		wait, stop := pacer.Pace(elapsed, count)
		if stop {
			break
		}

		time.Sleep(wait)

		// Create the job
		job := jobsPool.Get().(*common.Job)
		job.Id = count
		job.Timestamp = time.Now()

		// Try to run into actual worker pool
		select {
		case jobsCh <- job: // Not blocking try to put in channel
			count++
			continue
		default:
			workers.Add(1)
			go l.worker(&workers, jobsCh, &jobsPool)
		}
	}

	close(jobsCh)
	workers.Wait()

	runtime.GC()
}

func (l Loadastic) worker(workersCount *sync.WaitGroup, jobs <-chan *common.Job, jobsPool *sync.Pool) {
	defer workersCount.Done()
	for j := range jobs {
		// Create the request
		req := l.requestFactory(j.Timestamp, j.Id)

		l.beforeSend(req, j.Timestamp, j.Id)

		// Send the request
		res, err := l.sender.Send(req)

		if err != nil {
			l.afterFailed(req, err, j.Id)
			continue
		}

		// Check if failed
		err = l.failedChecker(res)
		if err != nil {
			l.afterFailed(req, err, j.Id)
			continue
		}

		l.afterSend(req, res, j.Id)

		jobsPool.Put(j)
	}
}