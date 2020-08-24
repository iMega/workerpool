package workerpool_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/imega/workerpool"
)

var ErrFailedAssert = errors.New("failed to assert value form request")

type Request struct {
	Value string
}

type Response struct {
	Value string
}

func testCallback(ctx context.Context, in *Request) (*Response, error) {
	_ = ctx

	if in.Value == "ping" {
		return &Response{
			Value: "pong",
		}, nil
	}

	return &Response{}, ErrFailedAssert
}

func TestWorkpool(t *testing.T) {
	const ChannelSize = 20
	pool := workerpool.NewWorkerPoolWithBuffer(ChannelSize)

	req := &Request{
		Value: "ping",
	}

	pool.AppendJob(
		req,
		workerpool.Callback(
			func(ctx context.Context, in interface{}) (interface{}, error) {
				return testCallback(ctx, in.(*Request))
			},
		),
	)

	actual, err := workerpool.Run(context.Background(), pool)
	if err != nil {
		t.Error("failed to run pool")
	}

	expected := []workerpool.ResultWorker{
		{
			Error: nil,
			Result: &Response{
				Value: "pong",
			},
		},
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Error("not equals")
	}
}
