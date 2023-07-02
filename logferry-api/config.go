package main

import (
	"log"

	"github.com/BoRuDar/configuration/v4"
)

type Config struct {
	TLS struct {
		Cert string `env:"TLS_CERT"`
		Key  string `env:"TLS_KEY"`
	}

	Kafka struct {
		CA   string `env:"KAFKA_CA" default:"/etc/kafka-ca/tls/ca.crt"`
		Cert string `env:"KAFKA_CERT" default:"/etc/kafka/tls/user.crt" `
		Key  string `env:"KAFKA_KEY" default:"/etc/kafka/tls/key.crt"`
	}
}

func LoadConfig() (Config, error) {
	cfg := Config{}

	configurator := configuration.New(
		&cfg,
		// order of execution will be preserved:
		configuration.NewFlagProvider(), // 1st
		configuration.NewEnvProvider(),
		// configuration.NewJSONFileProvider(fileName),
		configuration.NewDefaultProvider(), // Last
	)

	configurator.SetOptions(
		configuration.OnFailFnOpt(func(err error) {
			// log.Printf("config fail: %s", err)
			log.Println(err)
		}),
	)

	if err := configurator.InitValues(); err != nil {
		return cfg, err
	}

	return cfg, nil
}
