package main

import (
	"github.com/apache/pulsar/pulsar-function-go/pf"

	pulsarFlogoTrigger "github.com/wcn00/flogo-pulsar/function"
)

func main() {
	pf.Start(pulsarFlogoTrigger.Invoke)
}
