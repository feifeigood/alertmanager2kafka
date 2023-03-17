package config

import (
	"encoding/json"

	log "github.com/sirupsen/logrus"
)

type (
	Opts struct {
		// logger
		Logger struct {
			Debug   bool `           long:"debug"        env:"DEBUG"    description:"debug mode"`
			Verbose bool `short:"v"  long:"verbose"      env:"VERBOSE"  description:"verbose mode"`
			LogJson bool `           long:"log.json"     env:"LOG_JSON" description:"Switch log output to json format"`
		}

		// kafka
		Kafka struct {
			// Kafka settings
			Host      string `long:"kafka.host"       env:"KAFKA_HOST"       description:"Kafka host, eg. kafka-0:9092"      required:"true"`
			Topic     string `long:"kafka.topic"      env:"KAFKA_TOPIC"      description:"Kafka topic, eg. alertmanager"     required:"true"`
			SSLCert   string `long:"kafka.ssl.cert"   env:"KAFKA_SSL_CERT"   description:"Kafka client SSL certificate file" required:"false"`
			SSLKey    string `long:"kafka.ssl.key"    env:"KAFKA_SSL_KEY"    description:"Kafka client SSL key file"         required:"false"`
			SSLCACert string `long:"kafka.ssl.cacert" env:"KAFKA_SSL_CACERT" description:"Kafka server CA certificate file"  required:"false"`
			Username  string `long:"kafka.username"   env:"KAFKA_USERNAME"   description:"Kafka authentication username"     required:"false"`
			Password  string `long:"kafka.password"   env:"KAFKA_PASSWORD"   description:"kafka authentication password"     required:"false"`
		}

		// general options
		ServerBind string `long:"bind"     env:"SERVER_BIND"   description:"Server address"     default:":9097"`
	}
)

func (o *Opts) GetJson() []byte {
	jsonBytes, err := json.Marshal(o)
	if err != nil {
		log.Panic(err)
	}
	return jsonBytes
}
