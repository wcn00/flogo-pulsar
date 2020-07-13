package activity

import (
	"context"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/project-flogo/core/activity"
	"github.com/project-flogo/core/data/coerce"
	"github.com/project-flogo/core/data/metadata"
)

func init() {
	_ = activity.Register(&Activity{}, New)
}

var activityMd = activity.ToMetadata(&Settings{}, &Input{})

//New optional factory method, should be used if one activity instance per configuration is desired
func New(ctx activity.InitContext) (activity.Activity, error) {

	s := &Settings{}
	err := metadata.MapToStruct(ctx.Settings(), s, true)
	if err != nil {
		return nil, err
	}

	connManager, err := coerce.ToConnection(s.Connection)
	if err != nil {
		return nil, err
	}
	pulsarClient := connManager.GetConnection().(pulsar.Client)

	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
		Topic: ctx.Settings()["topic"].(string),
	})

	if err != nil {
		return nil, fmt.Errorf("Could not instantiate Pulsar producer: %v", err)
	}

	act := &Activity{producer: producer}

	return act, nil
}

// Activity is an sample Activity that can be used as a base to create a custom activity
type Activity struct {
	producer pulsar.Producer
}

// Metadata returns the activity's metadata
func (a *Activity) Metadata() *activity.Metadata {
	return activityMd
}

// Eval implements api.Activity.Eval - Logs the Message
func (a *Activity) Eval(ctx activity.Context) (done bool, err error) {

	input := &Input{}
	err = ctx.GetInputObject(input)
	if err != nil {
		return true, err
	}

	msg := pulsar.ProducerMessage{
		Payload: []byte(input.Payload),
	}

	msgID, err := a.producer.Send(context.Background(), &msg)
	if err != nil {
		return true, fmt.Errorf("Producer could not send message: %v", err)
	}
	fmt.Printf("PulsarActivity producer sent message ID: %x\n", msgID.Serialize())
	return true, nil
}
