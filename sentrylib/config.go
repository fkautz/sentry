package sentrylib

type Config struct {
	AprsServer      string
	AprsUser        string
	AprsPasscode    string
	AprsFilter      string
	Cutoff          string
	SkipCooldown    bool             `json:",omitempty"`
	Mailgun         *MailgunConfig   `json:",omitempty"`
	BoltConfig      *BoltConfig      `json:",omitempty"`
	PostgresConfig  *PostgresConfig  `json:",omitempty"`
	GoLevelDBConfig *GoLevelDbConfig `json:",omitempty"`
	RethinkDBConfig *RethinkConfig   `json:",omitempty"`
}

type MailgunConfig struct {
	Domain      string
	ApiKey      string
	PubApiKey   string
	FromAddress string
}

type BoltConfig struct {
	File string
}
type PostgresConfig struct {
	ConnString string `yaml:"connstring,omitempty" json:",omitempty"`
	User       string
	Password   string
	Host       string
	DbName     string
	SslMode    string
}
type RethinkConfig struct {
	Address  string
	Database string
	Username string
	Password string
}

type GoLevelDbConfig struct {
	File string
}
