// Package actor is generated by protoactor-go/protoc-gen-gograin@0.1.0
package actor

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
	plog = logmod.New(logmod.InfoLevel, "[GRAIN][actor]")
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
	ProcessSchedule(*SignalRequest, cluster.GrainContext) (*SignalResponse, error)
}

// SchedulerGrainClient holds the base data for the SchedulerGrain
type SchedulerGrainClient struct {
	Identity string
	cluster  *cluster.Cluster
}

// ProcessSchedule requests the execution on to the cluster with CallOptions
func (g *SchedulerGrainClient) ProcessSchedule(r *SignalRequest, opts ...cluster.GrainCallOption) (*SignalResponse, error) {
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
		result := &SignalResponse{}
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
			req := &SignalRequest{}
			err := proto.Unmarshal(msg.MessageData, req)
			if err != nil {
				plog.Error("ProcessSchedule(SignalRequest) proto.Unmarshal failed.", logmod.Error(err))
				resp := &cluster.GrainErrorResponse{Err: err.Error()}
				ctx.Respond(resp)
				return
			}
			r0, err := a.inner.ProcessSchedule(req, a.ctx)
			if err != nil {
				resp := &cluster.GrainErrorResponse{Err: err.Error()}
				ctx.Respond(resp)
				return
			}
			bytes, err := proto.Marshal(r0)
			if err != nil {
				plog.Error("ProcessSchedule(SignalRequest) proto.Marshal failed", logmod.Error(err))
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
