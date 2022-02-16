package config_test

import (
	"fmt"
	"testing"

	"github.com/deatheyes/mongo-proxy/config"
	"github.com/stretchr/testify/assert"
)

func TestConfigLoad(t *testing.T) {
	c, err := config.FromFile("./config.yaml")
	assert.Nil(t, err)
	s, err := config.ToString(c)
	assert.Nil(t, err)
	fmt.Println(s)
}
