package sentrylib

import (
	"time"
)

type Mail interface {
	Send(email, callsign string, ts time.Time) error
}
