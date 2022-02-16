package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Meta struct {
	Cluster *Backend `yaml:"cluster"`
	Auth    *Backend `yaml:"auth"`
}

type Mongo struct {
	URL      string   `yaml:"url,omitempty"`
	Servers  []string `yaml:"servers,omitempty"`
	Database string   `yaml:"database"`
}

type Backend struct {
	Kind  string `yaml:"kind"`
	Mongo *Mongo `yaml:"mongo,omitempty"`
}

type Connection struct {
	AliveTimeoutSecond uint `yaml:"aliveTimeout"`
}

type Config struct {
	Meta       *Meta       `yaml:"meta"`
	Connection *Connection `yaml:"connection"`
}

func FromFile(file string) (*Config, error) {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	c := &Config{}
	if err := yaml.Unmarshal(content, c); err != nil {
		return nil, err
	}
	return c, nil
}

func ToFile(c *Config, file string) error {
	data, err := yaml.Marshal(c)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(file, data, 0644)
}

func ToString(c *Config) (string, error) {
	data, err := yaml.Marshal(c)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
