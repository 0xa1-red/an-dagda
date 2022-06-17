package actor

import (
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	UUIDNamespace = uuid.MustParse("6ba7b812-9dad-11d1-80b4-00c04fd430c8")
	OverseerID    = uuid.NewSHA1(UUIDNamespace, []byte("actor.overseer"))
)

type SchedulerGrain struct{}

func (a SchedulerGrain) Init(ctx cluster.GrainContext)           {}
func (a SchedulerGrain) Terminate(ctx cluster.GrainContext)      {}
func (a SchedulerGrain) ReceiveDefault(ctx cluster.GrainContext) {}

func (a SchedulerGrain) ProcessSchedule(r *SignalRequest, ctx cluster.GrainContext) (*SignalResponse, error) {
	return &SignalResponse{Status: Status_OK, Error: "", Timestamp: timestamppb.Now()}, nil
}
