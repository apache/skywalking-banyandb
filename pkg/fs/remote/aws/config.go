package aws

import "time"

type AWSConfig struct {
	Region    string
	KeyID     string
	SecretKey string
	Endpoint  string
	Timeout   time.Duration
}

var awsGlobalConfig *AWSConfig

func SetAWSConfig(cfg *AWSConfig) {
	awsGlobalConfig = cfg
}

func getAWSConfig() *AWSConfig {
	return awsGlobalConfig
}
