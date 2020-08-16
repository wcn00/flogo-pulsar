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
	Key         interface{}       `md:"key"`
	Properties  map[string]string `md:"properties"`
	PayloadStr  interface{}       `md:"payloadStr"`
	PayloadJSON interface{}       `md:"payloadJSON"`
}

// FromMap frommap
func (r *Input) FromMap(values map[string]interface{}) (err error) {
	r.Key, err = coerce.ToString(values["key"])
	if err != nil {
		return
	}
	r.Properties, err = coerce.ToParams(values["properties"])
	if err != nil {
		return err
	}
	r.PayloadStr, err = coerce.ToString(values["payloadStr"])
	if err != nil {
		return
	}
	r.PayloadJSON, err = coerce.ToObject(values["payloadJSON"])
	if err != nil {
		return
	}
	return
}

// ToMap tomap
func (r *Input) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"key":         r.Key,
		"properties":  r.Properties,
		"payloadStr":  r.PayloadStr,
		"payloadJSON": r.PayloadJSON,
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
