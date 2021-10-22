package config

import (
	"os"
	"path/filepath"
)

var (
	CAFile         = getConfigFilePath("ca.pem")
	ServerCertFile = getConfigFilePath("server.pem")
	ServerKeyFile  = getConfigFilePath("server-key.pem")

	ClientCertFile = getConfigFilePath("client.pem")
	ClientKeyFile  = getConfigFilePath("client-key.pem")

	RootClientCertFile = getConfigFilePath("root-client.pem")
	RootClientKeyFile  = getConfigFilePath("root-client-key.pem")

	NobodyClientCertFile = getConfigFilePath("nobody-client.pem")
	NobodyClientKeyFile  = getConfigFilePath("nobody-client-key.pem")

	ACLModelFile  = getConfigFilePath("model.conf")
	ACLPolicyFile = getConfigFilePath("policy.conf")
)

func getConfigFilePath(relativePath string) string {
	configDir := ""
	if dir := os.Getenv("CONFIG_DIR"); dir != "" {
		configDir = dir
	} else {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			panic(err)
		}
		configDir = filepath.Join(homeDir, ".proglog")
	}

	return filepath.Join(configDir, relativePath)

}
