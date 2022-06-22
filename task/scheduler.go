package task

import (
	"context"
	"fmt"
	"time"

	"github.com/0xa1-red/an-dagda/backend"
	"github.com/0xa1-red/an-dagda/schedule"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/asynkron/protoactor-go/log"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	UUIDNamespace = uuid.MustParse("6ba7b812-9dad-11d1-80b4-00c04fd430c8")
	OverseerID    = uuid.NewSHA1(UUIDNamespace, []byte("actor.overseer"))
)

type SchedulerGrain struct {
	ctx      cluster.GrainContext
	t        *time.Ticker
	provider backend.Provider
}

func NewScheduler(provider backend.Provider) Scheduler {
	return &SchedulerGrain{
		provider: provider,
	}
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
			if err := a.processSchedule(); err != nil {
				plog.Error("Failed to process schedule", log.Error(err))
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

func (a *SchedulerGrain) Schedule(r *ScheduleRequest, ctx cluster.GrainContext) (*ScheduleResponse, error) {
	sctx, span := tracer.Start(context.Background(), "schedule")
	defer span.End()

	id := uuid.New()

	task := schedule.Task{
		ID:    id,
		Topic: r.Channel,
		Data:  r.Message,
	}
	span.SetAttributes(attribute.String("task-id", id.String()))
	if err := a.provider.Schedule(sctx, &task, r.ScheduleAt.AsTime()); err != nil {
		return &ScheduleResponse{Status: Status_Error, Error: err.Error()}, err
	}
	return &ScheduleResponse{Status: Status_OK}, nil
}

func (a *SchedulerGrain) processSchedule() error {
	sctx, span := tracer.Start(context.Background(), "process-schedule")
	defer span.End()
	tasks, err := a.provider.GetCurrentTasks(sctx)
	if err != nil && err.Error() != "EOF" {
		err = fmt.Errorf("Failed to get list of scheduled tasks: %w", err)
		span.RecordError(err)
		return err
	}

	if len(tasks) == 0 {
		plog.Debug("No new tasks")
		return nil
	}

	tc := propagation.TraceContext{}
	carrier := propagation.MapCarrier{}
	tc.Inject(sctx, carrier)

	traceID := carrier.Get(tc.Fields()[0])
	cleanup := []*schedule.Task{}
	for _, task := range tasks {
		if err := a.provider.LockTask(sctx, task, 1*time.Hour); err != nil {
			plog.Error("Error locking task", log.String("task-id", task.ID.String()))
			continue
		}
		taskGrain := GetTaskProcessorGrainClient(a.ctx.Cluster(), newTaskProcessorID())
		req := TaskRequest{
			TaskID:    task.ID.String(),
			TraceID:   traceID,
			Timestamp: timestamppb.Now(),
		}
		_, err := taskGrain.ProcessTask(&req)
		if err != nil {
			span.RecordError(err)
			plog.Error("Failed to process task", log.String("task-id", task.ID.String()), log.Error(err))
			return err
		}
		plog.Info("Task successfully processed", log.String("trace-id", traceID), log.String("task-id", task.ID.String()), log.String("raw-data", task.Data))
		cleanup = append(cleanup, task)
		if err := a.provider.UnlockTask(sctx, task); err != nil {
			plog.Error("Error locking task", log.String("task-id", task.ID.String()))
		}
	}

	for _, task := range cleanup {
		retries := 3
		success := false
		var err error
		for tries := 0; tries < retries; tries++ {
			sctx = context.WithValue(sctx, "task-id", task.ID.String())
			err = a.provider.Delete(sctx, task.Key())
			if err == nil {
				success = true
				break
			}
			if tries < retries-1 {
				plog.Warn("Failed to delete processed task, retrying", log.String("task-id", task.ID.String()), log.Error(err), log.Int("tries", tries))
				time.Sleep(100 * time.Millisecond)
			}
		}
		if !success {
			span.RecordError(err)
			plog.Error("Failed to delete processed task", log.String("task-id", task.ID.String()), log.Error(err))
		}

		plog.Info("Deleted processed task", log.String("task-id", task.ID.String()))
	}
	return nil
}

func init() {
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
