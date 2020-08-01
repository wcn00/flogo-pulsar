package connection

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/project-flogo/core/data/metadata"
	"github.com/project-flogo/core/support/connection"
)

func init() {
	connection.RegisterManager("connection", &PulsarConnection{})
	connection.RegisterManagerFactory(&Factory{})
}

// Settings comment
type Settings struct {
	Name          string `md:"name,required"`
	URL           string `md:"url,required"`
	CaCert        string `md:"cacert"`
	CertFile      string `md:"certFile"`
	KeyFile       string `md:"keyFile"`
	AllowInsecure bool   `md:"allowinsecure"`
}

// JWTToken             string `md:"jwttoken"`
// AthenzAuthentication string `md:"athenzauth"`

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
	keystoreDir, auth, err := getAuthentication(s)
	if err != nil {
		return nil, err
	}
	defer func() {
		if keystoreDir != "" {
			os.RemoveAll(keystoreDir)
		}
	}()
	clientOps := pulsar.ClientOptions{
		URL:                        s.URL,
		Authentication:             auth,
		TLSValidateHostname:        false,
		TLSAllowInsecureConnection: s.AllowInsecure,
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
	p.client.Close()
	return nil
}

// Start comment
func (p *PulsarConnection) Start() error {
	return nil
}

// ReleaseConnection comment
func (p *PulsarConnection) ReleaseConnection(connection interface{}) {
	p.Stop()
}

func getAuthentication(s *Settings) (keystoreDir string, auth pulsar.Authentication, err error) {
	keystoreDir, err = createTempKeystoreDir(s)
	if err != nil {
		return
	}
	if keystoreDir == "" {
		return "", nil, nil
	}
	auth = pulsar.NewAuthenticationTLS(keystoreDir+string(os.PathSeparator)+"certfile.pem",
		keystoreDir+string(os.PathSeparator)+"keyfile.pem")
	return
}

func createTempKeystoreDir(s *Settings) (keystoreDir string, err error) {
	var certObj, keyObj map[string]interface{}
	if s.CertFile == "" || s.KeyFile == "" {
		return "", nil
	}
	err = json.Unmarshal([]byte(s.CertFile), &certObj)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(s.KeyFile), &keyObj)
	if err != nil {
		return
	}
	certBytes, err := getBytesFromFileSetting(certObj)
	if err != nil {
		return
	}
	keyBytes, err := getBytesFromFileSetting(keyObj)
	if err != nil {
		return
	}
	keystoreDir, err = ioutil.TempDir(os.TempDir(), s.Name)
	if err != nil {
		return
	}
	err = ioutil.WriteFile(keystoreDir+string(os.PathSeparator)+"certfile.pem", certBytes, 0644)
	if err != nil {
		return
	}
	err = ioutil.WriteFile(keystoreDir+string(os.PathSeparator)+"keyfile.pem", keyBytes, 0644)
	if err != nil {
		return
	}
	return
}

func getBytesFromFileSetting(fileSetting map[string]interface{}) (destArray []byte, err error) {
	var header = "base64,"
	value := fileSetting["content"].(string)
	if value == "" {
		return nil, fmt.Errorf("file based setting contains no data")
	}

	if strings.Index(value, header) >= 0 {
		value = value[strings.Index(value, header)+len(header):]
		decodedLen := base64.StdEncoding.DecodedLen(len(value))
		destArray := make([]byte, decodedLen)
		actualen, err := base64.StdEncoding.Decode(destArray, []byte(value))
		if err != nil {
			return nil, fmt.Errorf("file based setting not base64 encoded: [%s]", err)
		}
		if decodedLen != actualen {
			newDestArray := make([]byte, actualen)
			copy(newDestArray, destArray)
			destArray = newDestArray
			return newDestArray, nil
		}
		return destArray, nil
	}
	return nil, fmt.Errorf("internal error; file based setting not formatted correctly")
}
