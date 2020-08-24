package workerpool

import (
	"context"
	"sync"
)

type pool struct {
	ChannelSize int
	Jobs        []Job
}

type inputGetter struct {
	Input interface{}
}

func (i *inputGetter) GetInput() interface{} {
	return i.Input
}

type job struct {
	CallBack Callback
	Input    InputGetter
}

func (j *job) InputGetter() InputGetter {
	return j.Input
}

func (j *job) GetCallback() Callback {
	return j.CallBack
}

func (j *job) GetInput() interface{} {
	return j.Input
}

type Callback func(context.Context, interface{}) (interface{}, error)

func (p *pool) AppendJob(in interface{}, cb Callback) {
	j := &job{
		Input: &inputGetter{
			Input: in,
		},
		CallBack: cb,
	}
	p.Jobs = append(p.Jobs, j)
}

func (p *pool) GetChannelSize() int {
	return p.ChannelSize
}

func (p *pool) GetJobs() []Job {
	return p.Jobs
}

func NewWorkerPoolWithBuffer(channelSize int) Pool {
	return &pool{
		ChannelSize: channelSize,
	}
}

type Job interface {
	GetCallback() Callback
	InputGetter() InputGetter
}

type InputGetter interface {
	GetInput() interface{}
}

type Pool interface {
	GetChannelSize() int
	GetJobs() []Job
	AppendJob(in interface{}, cb Callback)
}

type ResultWorker struct {
	Error  error
	Result interface{}
}

func Run(ctx context.Context, in Pool) ([]ResultWorker, error) {
	var (
		wg       = sync.WaitGroup{}
		resultCh = make(chan ResultWorker, in.GetChannelSize())
	)

	for _, job := range in.GetJobs() {
		wg.Add(1)

		go func(j Job) {
			defer wg.Done()
			resultCh <- worker(ctx, j)
		}(job)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	result := []ResultWorker{}

	for {
		select {
		case <-ctx.Done():
			return result, nil

		case r, ok := <-resultCh:
			if !ok {
				return result, nil
			}

			result = append(result, r)
		}
	}
}

func worker(ctx context.Context, job Job) ResultWorker {
	var (
		ch    = make(chan interface{}, 1)
		errCh = make(chan error, 1)
	)

	go func() {
		resp, err := job.GetCallback()(ctx, job.InputGetter().GetInput())
		if err != nil {
			errCh <- err

			return
		}
		ch <- resp
	}()

	select {
	case <-ctx.Done():
		go func() {
			for range ch {
			}

			close(ch)
			close(errCh)
		}()

		return ResultWorker{
			Error: ctx.Err(),
		}

	case msg := <-ch:
		close(ch)
		close(errCh)

		return ResultWorker{
			Result: msg,
		}

	case err := <-errCh:
		close(ch)
		close(errCh)

		return ResultWorker{
			Error: err,
		}
	}
}
