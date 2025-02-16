package config

import (
	"gopkg.in/yaml.v3"
	"os"
)

type Config struct {
	Enabled bool `yaml:"enabled"`

	Users []UserCredentials `yaml:"users"`
}

type UserCredentials struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

func NewConfig() *Config {
	return &Config{}
}

func LoadConfig(cfg *Config) error {
	data, err := os.ReadFile("banyand/liaison/pkg/config/config.yaml")
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(data, cfg)
	if err != nil {
		return err
	}
	return nil
}
