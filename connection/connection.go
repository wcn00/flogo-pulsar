package connection

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/project-flogo/core/data/metadata"
	"github.com/project-flogo/core/support/connection"
)

func init() {
	connection.RegisterManager("pulsarConnection", &PulsarConnection{})
	connection.RegisterManagerFactory(&Factory{})
}

// Settings comment
type Settings struct {
	URL                  string `md:"url,required"`
	AthenzAuthentication string `md:"athenzauth"`
}

// JWTToken             string `md:"jwttoken"`
// AthenzAuthentication string `md:"athenzauth"`
// CertFile             string `md:"certFile"`
// KeyFile              string `md:"keyFile"`
// CaCert               string `md:"cacert"`

// PulsarConnection comment
type PulsarConnection struct {
	client pulsar.Client
}

// Factory comment
type Factory struct {
}

// Type comment
func (*Factory) Type() string {
	return "pulsar"
}

// NewManager comment
func (*Factory) NewManager(settings map[string]interface{}) (connection.Manager, error) {
	s := &Settings{}
	err := metadata.MapToStruct(settings, s, true)
	if err != nil {
		return nil, err
	}

	auth := getAuthentication(s)

	clientOps := pulsar.ClientOptions{
		URL:            s.URL,
		Authentication: auth,
	}
	client, err := pulsar.NewClient(clientOps)

	if err != nil {
		return nil, err
	}

	return &PulsarConnection{client: client}, nil
}

// Type comment
func (p *PulsarConnection) Type() string {

	return "pulsar"
}

// GetConnection comment
func (p *PulsarConnection) GetConnection() interface{} {

	return p.client
}

// Stop comment
func (p *PulsarConnection) Stop() error {
	return nil
}

// Start comment
func (p *PulsarConnection) Start() error {
	return nil
}

// ReleaseConnection comment
func (p *PulsarConnection) ReleaseConnection(connection interface{}) {

}

func getAuthentication(s *Settings) pulsar.Authentication {
	if s.AthenzAuthentication != "" {
		return nil
		//return pulsar.NewAuthenticationAthenz(s.AthenzAuthentication)

	}
	// if s.CertFile != "" && s.KeyFile != "" {
	// 	return pulsar.NewAuthenticationTLS(s.CertFile, s.KeyFile)
	// }
	return nil
}
