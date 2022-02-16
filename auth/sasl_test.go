package auth_test

import (
	"testing"

	"github.com/deatheyes/mongo-proxy/auth"
	"github.com/stretchr/testify/assert"
)

func TestSalsFirstCommand(t *testing.T) {
	password := "password"
	username := "test_user"
	authenticator, err := auth.NewScramSHA256ClientAuthenticator(&auth.Cred{
		Source:   "test_db",
		Username: username,
		Password: password,
	})
	assert.Nil(t, err)
	cmd, err := auth.SaslFirstCommand("test_db", &auth.Config{}, authenticator.(*auth.ScramClientAuthenticator).CreateSaslClient())
	assert.Nil(t, err)
	_, err = cmd.Payload()
	assert.Nil(t, err)
}
