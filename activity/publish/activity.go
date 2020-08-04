package publish

import (
	"context"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/project-flogo/core/activity"
	"github.com/project-flogo/core/data"
	"github.com/project-flogo/core/data/coerce"
	"github.com/project-flogo/core/data/metadata"
	"github.com/project-flogo/core/support/log"
)

var logger = log.ChildLogger(log.RootLogger(), "flogo-pulsar-publish")

func init() {
	_ = activity.Register(&Activity{}, New)
}

var activityMd = activity.ToMetadata(&Settings{}, &Input{}, &Output{})

//New optional factory method, should be used if one activity instance per configuration is desired
func New(ctx activity.InitContext) (act activity.Activity, err error) {
	s := &Settings{}
	err = metadata.MapToStruct(ctx.Settings(), s, true)
	if err != nil {
		return
	}
	connManager, err := coerce.ToConnection(s.Connection)
	if err != nil {
		return
	}
	pulsarClient := connManager.GetConnection().(pulsar.Client)

	producerOptions := pulsar.ProducerOptions{
		Topic: ctx.Settings()["topic"].(string),
	}
	switch ctx.Settings()["compressiontype"].(string) {
	case ("LZ4"):
		producerOptions.CompressionType = pulsar.LZ4
	case ("ZLIB"):
		producerOptions.CompressionType = pulsar.ZLib
	case ("ZSTD"):
		producerOptions.CompressionType = pulsar.ZSTD
	default:
		producerOptions.CompressionType = pulsar.NoCompression
	}

	producer, err := pulsarClient.CreateProducer(producerOptions)
	if err != nil {
		return nil, fmt.Errorf("Could not instantiate Pulsar producer: %v", err)
	}
	act = &Activity{producer: producer}
	return
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
	logger.Debugf("publish eval called")
	input := &Input{}
	err = ctx.GetInputObject(input)
	if err != nil {
		return true, err
	}
	msgBytes, err := coerce.ToType(input.Payload, data.TypeBytes)
	if err != nil {
		return true, err
	}
	msg := pulsar.ProducerMessage{
		Payload: msgBytes.([]byte)
	}
	msgID, err := a.producer.Send(context.Background(), &msg)
	if err != nil {
		return true, fmt.Errorf("Producer could not send message: %v", err)
	}
	ctx.SetOutput("msgid", fmt.Sprintf("%x", msgID.Serialize()))
	return true, nil
}
