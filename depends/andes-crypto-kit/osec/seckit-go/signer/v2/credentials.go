package v2

// A Value is the AWS credentials value for individual credential fields.
type Value struct {
	// AWS Access key ID
	AccessKeyID string

	// AWS Secret Access Key
	SecretAccessKey string

	// AWS Session Token
	SessionToken string

	// Provider used to get credentials
	ProviderName string
}

type Credentials struct {
	creds Value
}

// NewCredentials returns a pointer to a new Credentials with the provider set.
func NewCredentials(creds Value) *Credentials {
	c := &Credentials{
		creds: creds,
	}
	return c
}

// NewStaticCredentials returns a pointer to a new Credentials object
// wrapping a static credentials value provider. Token is only required
// for temporary security credentials retrieved via STS, otherwise an empty
// string can be passed for this parameter.
func NewStaticCredentials(id, secret, token string) *Credentials {
	return NewCredentials(Value{
		AccessKeyID:     id,
		SecretAccessKey: secret,
		SessionToken:    token,
	})
}
