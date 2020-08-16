package subscriber

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/project-flogo/core/data/coerce"
	"github.com/project-flogo/core/data/metadata"
	"github.com/project-flogo/core/support/log"
	"github.com/project-flogo/core/trigger"
)

var triggerMd = trigger.NewMetadata(&Settings{}, &HandlerSettings{}, &Output{})

func init() {
	_ = trigger.Register(&Trigger{}, &Factory{})
}

type Trigger struct {
	client   pulsar.Client
	handlers []*Handler
}
type Handler struct {
	handler  trigger.Handler
	consumer pulsar.Consumer
	running  bool
}

type Factory struct {
}

var logger log.Logger

//New factory method to create a new trigger
func (*Factory) New(config *trigger.Config) (trigger.Trigger, error) {
	s := &Settings{}
	err := metadata.MapToStruct(config.Settings, s, true)
	if err != nil {
		return nil, err
	}
	pulsarConn, err := coerce.ToConnection(s.Connection)
	if err != nil {
		return nil, err
	}
	return &Trigger{client: pulsarConn.GetConnection().(pulsar.Client)}, nil
}

//Metadata interface implementation to get the metadata
func (f *Factory) Metadata() *trigger.Metadata {
	return triggerMd
}

// Metadata implements trigger.Trigger.Metadata
func (t *Trigger) Metadata() *trigger.Metadata {
	return triggerMd
}

//Initialize Setup the trigger and create the consumer
func (t *Trigger) Initialize(ctx trigger.InitContext) error {
	logger = ctx.Logger()
	// Init handlers
	for _, handler := range ctx.GetHandlers() {
		s := &HandlerSettings{}
		err := metadata.MapToStruct(handler.Settings(), s, true)
		if err != nil {
			return err
		}
		consumeroptions := pulsar.ConsumerOptions{
			Topic:            s.Topic,
			SubscriptionName: s.Subscription,
		}
		switch s.SubscriptionType {
		case "Exclusive":
			consumeroptions.Type = pulsar.Exclusive
		case "Shared":
			consumeroptions.Type = pulsar.Shared
		case "Failover":
			consumeroptions.Type = pulsar.Failover
		case "KeyShared":
			consumeroptions.Type = pulsar.KeyShared
		}
		if s.DLQTopic != "" {
			policy := pulsar.DLQPolicy{
				MaxDeliveries: uint32(s.DLQMaxDeliveries),
				Topic:         s.DLQTopic,
			}
			consumeroptions.DLQ = &policy
		}
		if s.InitialPosition == "Latest" {
			consumeroptions.SubscriptionInitialPosition = pulsar.SubscriptionPositionLatest
		} else {
			consumeroptions.SubscriptionInitialPosition = pulsar.SubscriptionPositionEarliest
		}
		consumer, err := t.client.Subscribe(consumeroptions)
		if err != nil {
			return err
		}
		t.handlers = append(t.handlers, &Handler{handler: handler, consumer: consumer, running: false})
	}
	return nil
}

// Start implements util.Managed.Start
func (t *Trigger) Start() error {
	for _, handler := range t.handlers {
		handler.running = true
		go consume(handler)
	}
	return nil
}

// Stop implements util.Managed.Stop
func (t *Trigger) Stop() error {
	for _, handler := range t.handlers {
		handler.running = false
	}
	return nil
}

func consume(handler *Handler) {
	for {
		msg, err := handler.consumer.Receive(context.Background())
		if err != nil {
			logger.Debugf("Error while recieveing message")
			return
		}
		out := &Output{}
		out.Message = string(msg.Payload())
		// Do something with the message
		_, err = handler.handler.Handle(context.Background(), out)
		if err == nil {
			// Message processed successfully
			handler.consumer.Ack(msg)
		} else {
			// Failed to process messages
			handler.consumer.Nack(msg)
		}
		if !handler.running {
			break
		}
	}
}
