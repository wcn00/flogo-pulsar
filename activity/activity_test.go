package activity

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/project-flogo/core/activity"
	"github.com/project-flogo/core/data/mapper"
	"github.com/project-flogo/core/data/resolve"
	"github.com/project-flogo/core/support/test"
	"github.com/stretchr/testify/assert"
	"github.com/wcn00/flogo-pulsar/connection"
)

var pulsarConGilJSON = []byte(`{
	"connection": {
		"id": "e1e890d0-de91-11e9-aef0-13201957902e",
		"name": "pulsar",
		"ref": "github.com/wcn00/messaging-contrib/pulsar/connection",
		"settings": {
			"name": "TestConnectionToGil",
			"description": "TestConnectionToGil",
			"url": "pulsar://gil:6650"
		}
	}
}
`)

func TestRegister(t *testing.T) {

	ref := activity.GetRef(&Activity{})
	act := activity.Get(ref)

	assert.NotNil(t, act)
}
func TestEval(t *testing.T) {
	var pulsarConFactory connection.Factory
	connectionJSON := make(map[string]interface{})
	err := json.Unmarshal([]byte(pulsarConGilJSON), &connectionJSON)
	if err != nil {
		fmt.Printf("Failed to unmarshal connection: %s\n", err)
	}
	assert.Nil(t, err)
	connection := connectionJSON["connection"].(map[string]interface{})
	pulsConn, err := pulsarConFactory.NewManager(connection["settings"].(map[string]interface{}))
	assert.Nil(t, err)
	settings := make(map[string]interface{})
	settings["connection"] = pulsConn
	settings["topic"] = "wcntopic"
	mf := mapper.NewFactory(resolve.GetBasicResolver())
	iCtx := test.NewActivityInitContext(settings, mf)
	act, err := New(iCtx)
	assert.Nil(t, err)
	tc := test.NewActivityContext(act.Metadata())
	tc.SetInput("payload", "mary had a little lamb")
	_, err = act.Eval(tc)
	assert.Nil(t, err)
}
