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
	"github.com/project-flogo/core/support/log"
)

var logger = log.ChildLogger(log.RootLogger(), "flogo-pulsar-connection")

func init() {
	connection.RegisterManager("connection", &PulsarConnection{})
	connection.RegisterManagerFactory(&Factory{})
}

// Settings comment
type Settings struct {
	Name          string `md:"name,required"`
	URL           string `md:"url,required"`
	CaCert        string `md:"cacert"`
	Auth          string `md:"auth"`
	CertFile      string `md:"certFile"`
	KeyFile       string `md:"keyFile"`
	JWT           string `md:"jwt"`
	AllowInsecure bool   `md:"allowinsecure"`
}

// JWTToken             string `md:"jwttoken"`
// AthenzAuthentication string `md:"athenzauth"`

// PulsarConnection comment
type PulsarConnection struct {
	client      pulsar.Client
	keystoreDir string
	clientOpts  pulsar.ClientOptions
	settings    *Settings
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
	var auth pulsar.Authentication
	keystoreDir, err := createTempKeystoreDir(s)
	if err != nil {
		return nil, err
	}

	if s.Auth == "TLS" {
		auth, err = getTLSAuthentication(keystoreDir, s)
		if err != nil {
			return nil, err
		}
	} else if s.Auth == "JWT" {
		auth, err = getJWTAuthentication(s)
		if err != nil {
			return nil, err
		}
	}
	clientOpts := pulsar.ClientOptions{
		URL:                        s.URL,
		Authentication:             auth,
		TLSValidateHostname:        false,
		TLSAllowInsecureConnection: s.AllowInsecure,
	}
	if strings.Index(s.URL, "pulsar+ssl") >= 0 {
		clientOpts.TLSTrustCertsFilePath = keystoreDir + string(os.PathSeparator) + "cacert.pem"
	}
	logger.Debugf("pulsar.ClientOptions: %v", clientOpts)

	client, err := pulsar.NewClient(clientOpts)
	if err != nil {
		return nil, err
	}
	return &PulsarConnection{client: client, keystoreDir: keystoreDir, clientOpts: clientOpts, settings: s}, nil
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
	// logger.Debugf("PulsarConnection.Stop()")
	// p.client.Close()
	// os.RemoveAll(p.keystoreDir)
	return nil
}

// Start comment
func (p *PulsarConnection) Start() (err error) {
	logger.Debugf("PulsarConnection.Start()")
	// p.keystoreDir, _, err = getAuthentication(p.settings)
	// if err != nil {
	// 	return
	// }
	// logger.Debugf("PulsarConnection.Start KeystoreDir: %s", p.keystoreDir)

	// p.client, err = pulsar.NewClient(p.clientOpts)
	return
}

// ReleaseConnection comment
func (p *PulsarConnection) ReleaseConnection(connection interface{}) {
	logger.Debugf("PulsarConnection.ReleaseConnection()")
	p.Stop()
}

func getTLSAuthentication(keystoreDir string, s *Settings) (auth pulsar.Authentication, err error) {
	auth = pulsar.NewAuthenticationTLS(keystoreDir+string(os.PathSeparator)+"certfile.pem",
		keystoreDir+string(os.PathSeparator)+"keyfile.pem")
	return
}
func getJWTAuthentication(s *Settings) (auth pulsar.Authentication, err error) {
	auth = pulsar.NewAuthenticationToken(s.JWT)
	return
}

func createTempKeystoreDir(s *Settings) (keystoreDir string, err error) {
	var certObj, keyObj map[string]interface{}
	logger.Debugf("createTempCertificateDir:  %v", *s)
	if s.CertFile != "" || s.KeyFile != "" || s.CaCert != "" {
		keystoreDir, err = ioutil.TempDir(os.TempDir(), s.Name)
		if err != nil {
			return
		}
	} else {
		return "", nil
	}
	if s.CaCert != "" {
		err = json.Unmarshal([]byte(s.CaCert), &certObj)
		if err != nil {
			return
		}
		var certBytes []byte
		certBytes, err = getBytesFromFileSetting(certObj)
		if err != nil {
			return
		}
		err = ioutil.WriteFile(keystoreDir+string(os.PathSeparator)+"cacert.pem", certBytes, 0644)
		if err != nil {
			return
		}
	}
	if s.CertFile != "" {
		err = json.Unmarshal([]byte(s.CertFile), &certObj)
		if err != nil {
			return
		}
		var certBytes []byte
		certBytes, err = getBytesFromFileSetting(certObj)
		if err != nil {
			return
		}
		err = ioutil.WriteFile(keystoreDir+string(os.PathSeparator)+"certfile.pem", certBytes, 0644)
		if err != nil {
			return
		}
	}
	if s.KeyFile != "" {
		err = json.Unmarshal([]byte(s.KeyFile), &keyObj)
		if err != nil {
			return
		}
		var keyBytes []byte
		keyBytes, err = getBytesFromFileSetting(keyObj)
		if err != nil {
			return
		}
		err = ioutil.WriteFile(keystoreDir+string(os.PathSeparator)+"keyfile.pem", keyBytes, 0644)
		if err != nil {
			return
		}
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
