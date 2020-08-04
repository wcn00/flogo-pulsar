package publish

import (
	"github.com/project-flogo/core/data/coerce"
	"github.com/project-flogo/core/support/connection"
)

// Settings Activity Settings
type Settings struct {
	Connection      connection.Manager `md:"connection"`
	Topic           string             `md:"topic,required"`
	CompressionType string             `md:"compressiontype"`
}

// Input to the publish activity
type Input struct {
	Payload interface{} `md:"payload,required"`
}

// FromMap frommap
func (r *Input) FromMap(values map[string]interface{}) error {
	var err error
	r.Payload, err = coerce.ToObject(values["payload"])
	return err
}

// ToMap tomap
func (r *Input) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"payload": r.Payload,
	}
}

// Output of the publish activity
type Output struct {
	Msgid string `md:"msgid"`
}

//FromMap frommap
func (o *Output) FromMap(values map[string]interface{}) (err error) {
	o.Msgid, err = coerce.ToString(values["msgid"])
	if err != nil {
		return
	}
	return
}

//ToMap tomap
func (o *Output) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"msgid": o.Msgid,
	}
}
