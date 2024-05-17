package main

import (
	"github.com/pkg/errors"
	uconfig "go.uber.org/config"
)

type (
	Config struct {
		Shards []Shard `yaml:"shards"`
		Port   uint64  `yaml:"port"`
	}

	Shard struct {
		ID          uint64 `yaml:"id"`
		StartHeight uint64 `yaml:"startHeight,omitempty"`
		EndHeight   uint64 `yaml:"endHeight,omitempty"`
		Endpoint    string `yaml:"endpoint,omitempty"`
	}
)

var (
	DefaultConfig = Config{
		Port: 5014,
		Shards: []Shard{
			{
				ID:          0,
				StartHeight: 1,
				EndHeight:   12000256,
				Endpoint:    "http://127.0.0.1:15100",
			},
			{
				ID:          1,
				StartHeight: 12000256,
				EndHeight:   13000256,
				Endpoint:    "http://127.0.0.1:15112",
			},
		},
	}
)

func newConfig(path string) (Config, error) {
	opts := make([]uconfig.YAMLOption, 0)
	opts = append(opts, uconfig.Static(DefaultConfig))
	if path != "" {
		opts = append(opts, uconfig.File(path))
	}
	yaml, err := uconfig.NewYAML(opts...)
	if err != nil {
		return Config{}, errors.Wrap(err, "failed to init config")
	}

	var cfg Config
	if err := yaml.Get(uconfig.Root).Populate(&cfg); err != nil {
		return Config{}, errors.Wrap(err, "failed to unmarshal YAML config to struct")
	}
	return cfg, nil
}
