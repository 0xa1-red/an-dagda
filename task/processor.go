package task

import (
	"log"

	"github.com/asynkron/protoactor-go/cluster"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TaskProcessorGrain struct {
}

func (a TaskProcessorGrain) Init(ctx cluster.GrainContext) {
	log.Printf("Initializing TaskProcessorGrain with ID %s", ctx.Self().Id)
}
func (a TaskProcessorGrain) Terminate(ctx cluster.GrainContext)      {}
func (a TaskProcessorGrain) ReceiveDefault(ctx cluster.GrainContext) {}

func (a TaskProcessorGrain) ProcessTask(r *TaskRequest, ctx cluster.GrainContext) (*TaskResponse, error) {
	return &TaskResponse{Status: Status_OK, Error: "", Timestamp: timestamppb.Now()}, nil
}
