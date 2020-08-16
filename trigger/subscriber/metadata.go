package subscriber

import (
	"github.com/project-flogo/core/data/coerce"
	"github.com/project-flogo/core/support/connection"
)

//Settings from Metadata interface
type Settings struct {
	Connection connection.Manager `md:"connection,required"`
}

//HandlerSettings for this trigger
type HandlerSettings struct {
	Topic            string `md:"topic,required"`
	Subscription     string `md:"subscription,required"`
	SubscriptionType string `md:"subscriptiontype"`
	InitialPosition  string `md:"initialposition"`
	DLQMaxDeliveries int    `md:"dlqmaxdeliveries"`
	DLQTopic         string `md:"dlqtopic"`
}

//Output for this trigger
type Output struct {
	Key        string            `md:"key"`
	Properties map[string]string `md:"properties"`
	Message    string            `md:"message"`
}

//FromMap from Metadata interface
func (o *Output) FromMap(values map[string]interface{}) error {

	var err error
	o.Message, err = coerce.ToString(values["message"])
	if err != nil {
		return err
	}
	o.Key, err = coerce.ToString(values["key"])
	if err != nil {
		return err
	}
	o.Properties, err = coerce.ToParams(values["properties"])
	if err != nil {
		return err
	}

	return nil
}

//ToMap from Metadata interface
func (o *Output) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"message":    o.Message,
		"key":        o.Key,
		"properties": o.Properties,
	}
}
