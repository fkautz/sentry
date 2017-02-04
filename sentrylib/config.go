package sentrylib

type Config struct {
	AprsServer   string
	AprsUser     string
	AprsPasscode string
	AprsFilter   string
	Cutoff       string
	Mailgun      *MailgunConfig "mailgun,omitempty"
}

type MailgunConfig struct {
	Domain    string
	ApiKey    string
	PubApiKey string
}
