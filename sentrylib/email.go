package sentrylib

import (
	"gopkg.in/mailgun/mailgun-go.v1"
	"log"
	"time"
)

type Mail interface {
	Send(email, callsign string, ts time.Time) error
}

type mailgunWrapper struct {
	mg    mailgun.Mailgun
	store Store
}

func NewMailgunServer(config Config) Mail {
	mg := mailgun.NewMailgun(config.Mailgun.Domain, config.Mailgun.ApiKey, config.Mailgun.PubApiKey)
	return &mailgunWrapper{
		mg: mg,
	}
}

func (mail *mailgunWrapper) Send(email, callsign string, ts time.Time) error {
	body := "Hello, your APRS node '" + callsign + "' appears to be down as of " + ts.UTC().String() + "\n\n"
	body = body + "To see your most recently sent packets, please see:\n" +
		"http://aprs.fi/?c=raw&call=" + callsign
	msg := mail.mg.NewMessage("T2L Alert Service <sentry@t2l.org>",
		callsign+" appears to be down",
		body,
		email)
	resp, id, err := mail.mg.Send(msg)
	if err != nil {
		log.Println(err)
	}
	log.Printf("ID: %s Resp: %s\n", id, resp)
	return nil
}
