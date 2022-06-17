package task

import (
	"fmt"
	"log"
	"time"

	"github.com/asynkron/protoactor-go/cluster"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	UUIDNamespace = uuid.MustParse("6ba7b812-9dad-11d1-80b4-00c04fd430c8")
	OverseerID    = uuid.NewSHA1(UUIDNamespace, []byte("actor.overseer"))
)

type SchedulerGrain struct {
	ctx cluster.GrainContext
	t   *time.Ticker
}

func (a *SchedulerGrain) Init(ctx cluster.GrainContext) {
	a.ctx = ctx
}
func (a *SchedulerGrain) Terminate(ctx cluster.GrainContext) {
	if a.t != nil {
		a.t.Stop()
	}
}
func (a *SchedulerGrain) ReceiveDefault(ctx cluster.GrainContext) {}

func (a *SchedulerGrain) Start(r *Empty, ctx cluster.GrainContext) (*Empty, error) {
	a.t = time.NewTicker(time.Second)
	go func() {
		for range a.t.C {
			log.Println("Hello")
			if err := a.processSchedule(); err != nil {
				log.Printf("ERROR: %v", err)
			}
		}
	}()

	return nil, nil
}

func (a *SchedulerGrain) Stop(r *Empty, ctx cluster.GrainContext) (*Empty, error) {
	if a.t == nil {
		return nil, fmt.Errorf("Scheduler grain has not been started")
	}

	a.t.Stop()
	return nil, nil
}

func (a *SchedulerGrain) processSchedule() error {
	ids := []uuid.UUID{
		uuid.New(),
		uuid.New(),
		uuid.New(),
	}

	traceID := uuid.New()
	for _, id := range ids {
		taskGrain := GetTaskProcessorGrainClient(a.ctx.Cluster(), newTaskProcessorID())
		req := TaskRequest{
			TaskID:    id.String(),
			TraceID:   traceID.String(),
			Timestamp: timestamppb.Now(),
		}
		_, err := taskGrain.ProcessTask(&req)
		if err != nil {
			log.Printf("ERROR: %v", err)
			return err
		}
		log.Printf("Task %s was successfully processed", id)
	}
	return nil
}

func init() {
	log.Println("init")
	SchedulerFactory(func() Scheduler {
		return &SchedulerGrain{}
	})

	TaskProcessorFactory(func() TaskProcessor {
		return &TaskProcessorGrain{}
	})
}

func newTaskProcessorID() string {
	return "actor.task." + uuid.New().String()
}
