package task

import (
	"math/rand"
	"time"

	"github.com/0xa1-red/an-dagda/backend"
	"github.com/0xa1-red/an-dagda/instrumentation"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/asynkron/protoactor-go/log"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TaskProcessorGrain struct {
	provider backend.Provider
}

func (a TaskProcessorGrain) Init(ctx cluster.GrainContext) {
	plog.Info("Initializing TaskProcessorGrain", log.String("id", ctx.Self().Id))
}
func (a TaskProcessorGrain) Terminate(ctx cluster.GrainContext)      {}
func (a TaskProcessorGrain) ReceiveDefault(ctx cluster.GrainContext) {}

func (a TaskProcessorGrain) ProcessTask(r *TaskRequest, ctx cluster.GrainContext) (*TaskResponse, error) {
	_, span := instrumentation.StartFromRemote(r, tracer, "process-task")
	span.SetAttributes(attribute.String("task-id", r.TaskID))
	defer span.End()

	time.Sleep(time.Duration(rand.Intn(300)) * time.Millisecond)

	return &TaskResponse{Status: Status_OK, Error: "", Timestamp: timestamppb.Now()}, nil
}

func NewTaskProcessor() TaskProcessor {
	return &TaskProcessorGrain{}
}
