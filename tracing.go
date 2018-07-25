package messaging

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/satori/go.uuid"
	"github.com/uber/jaeger-client-go"
	config "github.com/uber/jaeger-client-go/config"
	events "github.com/veritone/core-messages/generated/go/events"
	"go.uber.org/zap"
)

// JaegerAgentHostPort point to the agent deployed with sidecar pattern
// Deployment should use the default port
const JaegerAgentHostPort = "0.0.0.0:6831"

// var messagingTracer opentracing.Tracer

type Tracer struct {
	opentracing.Tracer
	serviceName string
	logger      Logger
}

func AddTracer(serviceName, env string) (*Tracer, error) {
	if serviceName == "" {
		if serviceName = os.Getenv("SERVICE_NAME"); serviceName == "" {
			var err error
			serviceName, err = os.Hostname()
			if err != nil {
				return nil, err
			}
		}
	}
	if env == "" {
		env, _ = os.LookupEnv("LOGGER")
	}

	var sampler *config.SamplerConfig
	switch strings.ToLower(env) {
	case "prod":
		sampler = &config.SamplerConfig{
			Type:  "probabilistic",
			Param: 0.01, // 1% of traffic in production
		}
	default:
		sampler = &config.SamplerConfig{
			Type:  "probabilistic",
			Param: 1.0, // 100% of traffic in other envs
		}
	}
	cfg := config.Configuration{
		ServiceName: serviceName,
		Sampler:     sampler,
		Reporter: &config.ReporterConfig{
			LogSpans:            false,
			BufferFlushInterval: 1 * time.Second,
			LocalAgentHostPort:  JaegerAgentHostPort,
		},
	}
	logger, err := AddLogger(env)
	if err != nil {
		return nil, err
	}
	tracer, _, err := cfg.NewTracer(config.Logger(jaeger.StdLogger))
	return &Tracer{
		Tracer:      tracer,
		serviceName: serviceName,
		logger:      logger,
	}, err
}

func MustAddTracer(serviceName, env string) *Tracer {
	tracer, err := AddTracer(serviceName, env)
	if err != nil {
		log.Panic(err)
	}
	return tracer
}

func (t *Tracer) Trace(to *events.VtEvent, from *events.VtEvent) {
	var (
		optName         string
		startTime       time.Time
		err             error
		fromSpanContext opentracing.SpanContext
	)
	optName = to.Core.Name
	if from != nil {
		carrier := opentracing.TextMapCarrier(from.Trace.TraceContext)
		fromSpanContext, err = t.Extract(opentracing.TextMap, carrier)
		if err != nil {
			if err != opentracing.ErrSpanContextNotFound {
				t.logger.Warn("error extracting SpanContext",
					zap.String("id", from.Core.GetId()),
					zap.String("event", from.Core.GetName()),
					zap.String("err", err.Error()))
			}
		}
		startTime, err = time.Parse(time.RFC1123, from.Core.Timestamp)
	} else {
		startTime, err = time.Parse(time.RFC1123, to.Core.Timestamp)
	}
	if err != nil {
		startTime = time.Now()
	}

	traceStartTime := opentracing.StartTime(startTime)
	spanReference := opentracing.FollowsFrom(fromSpanContext)
	currSpan := t.StartSpan(optName, traceStartTime, spanReference)
	defer currSpan.Finish()

	traceCarrier := opentracing.TextMapCarrier(to.Trace.TraceContext)
	err = t.Inject(currSpan.Context(), opentracing.TextMap, traceCarrier)
	if err != nil {
		t.logger.Warn("error injecting SpanContext to new message",
			zap.String("id", to.Core.GetId()),
			zap.String("event", to.Core.GetName()),
			zap.String("err", err.Error()))
	}
}

func (t *Tracer) Decorate(msgType interface{}, msgPayload []byte) *events.VtEvent {
	protoMsg, ok := msgType.(proto.Message)
	if ok {
		if vtEventProtoMsg, ok := protoMsg.(*events.VtEvent); ok {
			t.logger.Debug("message is already an events.VtEvent type")
			return vtEventProtoMsg
		}
		eventName := proto.MessageName(protoMsg)
		t.logger.Debug("eventName:", zap.String("eventName", eventName))
		return &events.VtEvent{
			Core: &events.Core{
				Id:        uuid.Must(uuid.NewV4()).String(),
				Name:      eventName,
				Timestamp: time.Now().Format(time.RFC1123),
			},
			Trace: &events.Trace{
				TraceContext: make(map[string]string),
				ServiceName:  t.serviceName,
				TraceTags:    make(map[string]string),
			},
			Data: &any.Any{
				TypeUrl: "veritone.com/" + eventName,
				Value:   msgPayload,
			},
		}
	}
	return nil
}
