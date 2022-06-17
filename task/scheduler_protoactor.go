// Package task is generated by protoactor-go/protoc-gen-gograin@0.1.0
package task

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	logmod "github.com/asynkron/protoactor-go/log"
	"google.golang.org/protobuf/proto"
)

var (
	plog = logmod.New(logmod.InfoLevel, "[GRAIN][task]")
	_    = proto.Marshal
	_    = fmt.Errorf
	_    = math.Inf
)

// SetLogLevel sets the log level.
func SetLogLevel(level logmod.Level) {
	plog.SetLevel(level)
}

var xSchedulerFactory func() Scheduler

// SchedulerFactory produces a Scheduler
func SchedulerFactory(factory func() Scheduler) {
	xSchedulerFactory = factory
}

// GetSchedulerGrainClient instantiates a new SchedulerGrainClient with given Identity
func GetSchedulerGrainClient(c *cluster.Cluster, id string) *SchedulerGrainClient {
	if c == nil {
		panic(fmt.Errorf("nil cluster instance"))
	}
	if id == "" {
		panic(fmt.Errorf("empty id"))
	}
	return &SchedulerGrainClient{Identity: id, cluster: c}
}

// GetSchedulerKind instantiates a new cluster.Kind for Scheduler
func GetSchedulerKind(opts ...actor.PropsOption) *cluster.Kind {
	props := actor.PropsFromProducer(func() actor.Actor {
		return &SchedulerActor{
			Timeout: 60 * time.Second,
		}
	}, opts...)
	kind := cluster.NewKind("Scheduler", props)
	return kind
}

// GetSchedulerKind instantiates a new cluster.Kind for Scheduler
func NewSchedulerKind(factory func() Scheduler, timeout time.Duration, opts ...actor.PropsOption) *cluster.Kind {
	xSchedulerFactory = factory
	props := actor.PropsFromProducer(func() actor.Actor {
		return &SchedulerActor{
			Timeout: timeout,
		}
	}, opts...)
	kind := cluster.NewKind("Scheduler", props)
	return kind
}

// Scheduler interfaces the services available to the Scheduler
type Scheduler interface {
	Init(ctx cluster.GrainContext)
	Terminate(ctx cluster.GrainContext)
	ReceiveDefault(ctx cluster.GrainContext)
	Start(*Empty, cluster.GrainContext) (*Empty, error)
	Stop(*Empty, cluster.GrainContext) (*Empty, error)
}

// SchedulerGrainClient holds the base data for the SchedulerGrain
type SchedulerGrainClient struct {
	Identity string
	cluster  *cluster.Cluster
}

// Start requests the execution on to the cluster with CallOptions
func (g *SchedulerGrainClient) Start(r *Empty, opts ...cluster.GrainCallOption) (*Empty, error) {
	bytes, err := proto.Marshal(r)
	if err != nil {
		return nil, err
	}
	reqMsg := &cluster.GrainRequest{MethodIndex: 0, MessageData: bytes}
	resp, err := g.cluster.Call(g.Identity, "Scheduler", reqMsg, opts...)
	if err != nil {
		return nil, err
	}
	switch msg := resp.(type) {
	case *cluster.GrainResponse:
		result := &Empty{}
		err = proto.Unmarshal(msg.MessageData, result)
		if err != nil {
			return nil, err
		}
		return result, nil
	case *cluster.GrainErrorResponse:
		return nil, errors.New(msg.Err)
	default:
		return nil, errors.New("unknown response")
	}
}

// Stop requests the execution on to the cluster with CallOptions
func (g *SchedulerGrainClient) Stop(r *Empty, opts ...cluster.GrainCallOption) (*Empty, error) {
	bytes, err := proto.Marshal(r)
	if err != nil {
		return nil, err
	}
	reqMsg := &cluster.GrainRequest{MethodIndex: 1, MessageData: bytes}
	resp, err := g.cluster.Call(g.Identity, "Scheduler", reqMsg, opts...)
	if err != nil {
		return nil, err
	}
	switch msg := resp.(type) {
	case *cluster.GrainResponse:
		result := &Empty{}
		err = proto.Unmarshal(msg.MessageData, result)
		if err != nil {
			return nil, err
		}
		return result, nil
	case *cluster.GrainErrorResponse:
		return nil, errors.New(msg.Err)
	default:
		return nil, errors.New("unknown response")
	}
}

// SchedulerActor represents the actor structure
type SchedulerActor struct {
	ctx     cluster.GrainContext
	inner   Scheduler
	Timeout time.Duration
}

// Receive ensures the lifecycle of the actor for the received message
func (a *SchedulerActor) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *actor.Started: //pass
	case *cluster.ClusterInit:
		a.ctx = cluster.NewGrainContext(ctx, msg.Identity, msg.Cluster)
		a.inner = xSchedulerFactory()
		a.inner.Init(a.ctx)

		if a.Timeout > 0 {
			ctx.SetReceiveTimeout(a.Timeout)
		}
	case *actor.ReceiveTimeout:
		ctx.Poison(ctx.Self())
	case *actor.Stopped:
		a.inner.Terminate(a.ctx)
	case actor.AutoReceiveMessage: // pass
	case actor.SystemMessage: // pass

	case *cluster.GrainRequest:
		switch msg.MethodIndex {
		case 0:
			req := &Empty{}
			err := proto.Unmarshal(msg.MessageData, req)
			if err != nil {
				plog.Error("Start(Empty) proto.Unmarshal failed.", logmod.Error(err))
				resp := &cluster.GrainErrorResponse{Err: err.Error()}
				ctx.Respond(resp)
				return
			}
			r0, err := a.inner.Start(req, a.ctx)
			if err != nil {
				resp := &cluster.GrainErrorResponse{Err: err.Error()}
				ctx.Respond(resp)
				return
			}
			bytes, err := proto.Marshal(r0)
			if err != nil {
				plog.Error("Start(Empty) proto.Marshal failed", logmod.Error(err))
				resp := &cluster.GrainErrorResponse{Err: err.Error()}
				ctx.Respond(resp)
				return
			}
			resp := &cluster.GrainResponse{MessageData: bytes}
			ctx.Respond(resp)
		case 1:
			req := &Empty{}
			err := proto.Unmarshal(msg.MessageData, req)
			if err != nil {
				plog.Error("Stop(Empty) proto.Unmarshal failed.", logmod.Error(err))
				resp := &cluster.GrainErrorResponse{Err: err.Error()}
				ctx.Respond(resp)
				return
			}
			r0, err := a.inner.Stop(req, a.ctx)
			if err != nil {
				resp := &cluster.GrainErrorResponse{Err: err.Error()}
				ctx.Respond(resp)
				return
			}
			bytes, err := proto.Marshal(r0)
			if err != nil {
				plog.Error("Stop(Empty) proto.Marshal failed", logmod.Error(err))
				resp := &cluster.GrainErrorResponse{Err: err.Error()}
				ctx.Respond(resp)
				return
			}
			resp := &cluster.GrainResponse{MessageData: bytes}
			ctx.Respond(resp)

		}
	default:
		a.inner.ReceiveDefault(a.ctx)
	}
}

var xTaskProcessorFactory func() TaskProcessor

// TaskProcessorFactory produces a TaskProcessor
func TaskProcessorFactory(factory func() TaskProcessor) {
	xTaskProcessorFactory = factory
}

// GetTaskProcessorGrainClient instantiates a new TaskProcessorGrainClient with given Identity
func GetTaskProcessorGrainClient(c *cluster.Cluster, id string) *TaskProcessorGrainClient {
	if c == nil {
		panic(fmt.Errorf("nil cluster instance"))
	}
	if id == "" {
		panic(fmt.Errorf("empty id"))
	}
	return &TaskProcessorGrainClient{Identity: id, cluster: c}
}

// GetTaskProcessorKind instantiates a new cluster.Kind for TaskProcessor
func GetTaskProcessorKind(opts ...actor.PropsOption) *cluster.Kind {
	props := actor.PropsFromProducer(func() actor.Actor {
		return &TaskProcessorActor{
			Timeout: 60 * time.Second,
		}
	}, opts...)
	kind := cluster.NewKind("TaskProcessor", props)
	return kind
}

// GetTaskProcessorKind instantiates a new cluster.Kind for TaskProcessor
func NewTaskProcessorKind(factory func() TaskProcessor, timeout time.Duration, opts ...actor.PropsOption) *cluster.Kind {
	xTaskProcessorFactory = factory
	props := actor.PropsFromProducer(func() actor.Actor {
		return &TaskProcessorActor{
			Timeout: timeout,
		}
	}, opts...)
	kind := cluster.NewKind("TaskProcessor", props)
	return kind
}

// TaskProcessor interfaces the services available to the TaskProcessor
type TaskProcessor interface {
	Init(ctx cluster.GrainContext)
	Terminate(ctx cluster.GrainContext)
	ReceiveDefault(ctx cluster.GrainContext)
	ProcessTask(*TaskRequest, cluster.GrainContext) (*TaskResponse, error)
}

// TaskProcessorGrainClient holds the base data for the TaskProcessorGrain
type TaskProcessorGrainClient struct {
	Identity string
	cluster  *cluster.Cluster
}

// ProcessTask requests the execution on to the cluster with CallOptions
func (g *TaskProcessorGrainClient) ProcessTask(r *TaskRequest, opts ...cluster.GrainCallOption) (*TaskResponse, error) {
	bytes, err := proto.Marshal(r)
	if err != nil {
		return nil, err
	}
	reqMsg := &cluster.GrainRequest{MethodIndex: 0, MessageData: bytes}
	resp, err := g.cluster.Call(g.Identity, "TaskProcessor", reqMsg, opts...)
	if err != nil {
		return nil, err
	}
	switch msg := resp.(type) {
	case *cluster.GrainResponse:
		result := &TaskResponse{}
		err = proto.Unmarshal(msg.MessageData, result)
		if err != nil {
			return nil, err
		}
		return result, nil
	case *cluster.GrainErrorResponse:
		return nil, errors.New(msg.Err)
	default:
		return nil, errors.New("unknown response")
	}
}

// TaskProcessorActor represents the actor structure
type TaskProcessorActor struct {
	ctx     cluster.GrainContext
	inner   TaskProcessor
	Timeout time.Duration
}

// Receive ensures the lifecycle of the actor for the received message
func (a *TaskProcessorActor) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *actor.Started: //pass
	case *cluster.ClusterInit:
		a.ctx = cluster.NewGrainContext(ctx, msg.Identity, msg.Cluster)
		a.inner = xTaskProcessorFactory()
		a.inner.Init(a.ctx)

		if a.Timeout > 0 {
			ctx.SetReceiveTimeout(a.Timeout)
		}
	case *actor.ReceiveTimeout:
		ctx.Poison(ctx.Self())
	case *actor.Stopped:
		a.inner.Terminate(a.ctx)
	case actor.AutoReceiveMessage: // pass
	case actor.SystemMessage: // pass

	case *cluster.GrainRequest:
		switch msg.MethodIndex {
		case 0:
			req := &TaskRequest{}
			err := proto.Unmarshal(msg.MessageData, req)
			if err != nil {
				plog.Error("ProcessTask(TaskRequest) proto.Unmarshal failed.", logmod.Error(err))
				resp := &cluster.GrainErrorResponse{Err: err.Error()}
				ctx.Respond(resp)
				return
			}
			r0, err := a.inner.ProcessTask(req, a.ctx)
			if err != nil {
				resp := &cluster.GrainErrorResponse{Err: err.Error()}
				ctx.Respond(resp)
				return
			}
			bytes, err := proto.Marshal(r0)
			if err != nil {
				plog.Error("ProcessTask(TaskRequest) proto.Marshal failed", logmod.Error(err))
				resp := &cluster.GrainErrorResponse{Err: err.Error()}
				ctx.Respond(resp)
				return
			}
			resp := &cluster.GrainResponse{MessageData: bytes}
			ctx.Respond(resp)

		}
	default:
		a.inner.ReceiveDefault(a.ctx)
	}
}
