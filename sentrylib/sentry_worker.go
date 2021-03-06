package sentrylib

import (
	"errors"
	"fmt"
	"github.com/dustin/go-aprs"
	"github.com/fkautz/sentry/sentrylib/sentry_store"
	"log"
	"time"
)

type SentryWorker interface {
	HandleMessage(frame aprs.Frame) error
	ReapLiveNodes() ([]sentry_store.CallsignTime, error)
	Email(callsign string, ts time.Time)
	LastSeen() (time.Time, error)
}

type sentryWorker struct {
	store    sentry_store.Store
	duration time.Duration
	mail     Mail
}

var FrameNotValidError error = errors.New("Frame Not Valid")
var EmptyCallsignError error = errors.New("No Callsign")

func NewSentryWorker(store sentry_store.Store, liveDuration time.Duration, mail Mail) SentryWorker {
	return &sentryWorker{
		store:    store,
		duration: liveDuration,
		mail:     mail,
	}
}

func (worker *sentryWorker) HandleMessage(frame aprs.Frame) error {
	if !frame.IsValid() {
		return FrameNotValidError
	}

	callsign := frame.Source.String()
	if callsign == "" {
		return EmptyCallsignError
	}

	ts, ok, err := worker.store.GetLive(callsign)
	if err != nil {
		return err
	}
	pos, err := frame.Body.Position()
	if err != nil {
		return err
	}

	worker.store.RemoveDead(callsign)
	worker.store.AddLive(callsign)
	now := time.Now()

	symbol := pos.Symbol.Glyph()
	count, err := worker.store.CountLive()
	if err != nil {
		return err
	}
	if len(symbol) == 0 {
		symbol = " "
	}
	result := fmt.Sprintf("%d: %s\t%s", count, symbol, callsign)
	if len(callsign) < 8 {
		result += "\t"
	}
	if ok {
		log.Printf("%s\t%s\n", result, now.Sub(ts).String())
	} else {
		log.Println(result)
	}
	return nil
}

func (worker *sentryWorker) ReapLiveNodes() ([]sentry_store.CallsignTime, error) {
	duration := -1 * worker.duration
	cutoff := time.Now().Add(duration)
	nodes, err := worker.store.ListLive(cutoff)
	if err != nil {
		return nil, err
	}

	for k, v := range nodes {
		log.Println("Reaping:", k, v)
		worker.store.RemoveLive(v.Callsign, cutoff)
		worker.store.AddDead(v.Callsign, v.LastSeen)
	}

	return nodes, nil
}

func (worker *sentryWorker) Email(callsign string, ts time.Time) {
	email, ok, err := worker.store.GetEmail(callsign)
	if err != nil {
		log.Println(err)
	}
	if ok {
		err = worker.mail.Send(email, callsign, ts)
		if err != nil {
			log.Println(err)
		}
	}
}

func (worker *sentryWorker) LastSeen() (time.Time, error) {
	return worker.store.LastSeenLive()
}
