package sentrylib

import "time"

type Store interface {
	EmailAddressStore
	EntryStore
}

type CallsignTime struct {
	Callsign string
	LastSeen time.Time
}

type CallsignEmail struct {
	Callsign string
	Email    string
}

type EntryStore interface {
	AddLive(callsign string) error
	CountLive() (int, error)
	GetLive(callsign string) (time.Time, bool, error)
	ListLive(ts time.Time) ([]CallsignTime, error)
	RemoveLive(callsign string, ts time.Time) error

	AddDead(callsign string, ts time.Time) error
	CountDead() (int, error)
	GetDead(callsign string) (time.Time, bool, error)
	ListDead() ([]CallsignTime, error)
	RemoveDead(callsign string) error
}

type EmailAddressStore interface {
	AddEmail(callsign, email string) error
	GetEmail(callsign string) (string, bool, error)
	ListEmail() ([]CallsignEmail, error)
	RemoveEmail(callsign string) error
}
