package subscriber

import (
	"encoding/json"
	"testing"

	"github.com/project-flogo/core/action"
	"github.com/project-flogo/core/support/test"
	"github.com/project-flogo/core/trigger"
	"github.com/stretchr/testify/assert"
	_ "github.com/wcn00/pulsar/connection"
)

const testConfig string = `{
	"id": "pulsar",
	"ref": " github.com/wcn00/pulsar/trigger",
	"settings": {
	  "connection": {
		  "ref": "github.com/wcn00/pulsar/connection",
		  "settings":{
			"url": "pulsar://gil:6605"
		  }
	  }
	},
	"handlers": [
	  {
			"action":{
				"id":"dummy"
			},
			"settings": {
			  "topic": "wcntopic",
			  "subscription":"wcntopic-sub"
			}
	  }
	]

  }`

func TestPulsarTrigger_Initialize(t *testing.T) {
	f := &Factory{}

	config := &trigger.Config{}
	err := json.Unmarshal([]byte(testConfig), config)
	assert.Nil(t, err)

	actions := map[string]action.Action{"dummy": test.NewDummyAction(func() {
		//do nothing
	})}

	trg, err := test.InitTrigger(f, config, actions)
	assert.Nil(t, err)
	assert.NotNil(t, trg)

	err = trg.Start()
	assert.Nil(t, err)

}
