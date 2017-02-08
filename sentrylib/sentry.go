package sentrylib

import (
	"errors"
	"io"
	"log"
	"time"
)

type Sentry interface {
	Serve() error
}

type sentry struct {
	config Config
}

func NewSentry(config Config) Sentry {
	return &sentry{
		config: config,
	}
}

func (server *sentry) Serve() error {
	log.SetFlags(log.Flags() | log.Llongfile)
	client := NewAprsClient(server.config.AprsServer, server.config.AprsUser, server.config.AprsPasscode, server.config.AprsFilter)

	store, err := NewBoltStore("sentry.db")
	//store, err := NewRethinkDB("localhost", "dev")
	//store, err := NewGoLevelDB("level.db")
	if err != nil {
		return err
	}

	mail := NewMailgunServer(server.config)

	// runs in background
	NewWebServer(store)

	duration := 25 * time.Hour
	if server.config.Cutoff != "" {
		duration, err = time.ParseDuration(server.config.Cutoff)
		if err != nil {
			return errors.New("Unable to parse Cutoff in config")
		}
	}

	worker := NewSentryWorker(store, duration, mail)

	go RunReaper(worker, duration, server.config.SkipCooldown)

	for {
		err = client.Dial()
		if err != nil {
			return err
		}
		count := 0
		totalTime := 0 * time.Second
		for client.Next() {
			frame, err := client.Frame()
			if err != nil {
				log.Println(err)
			}
			ts1 := time.Now()
			err = worker.HandleMessage(frame)
			ts2 := time.Now()
			dur := ts2.Sub(ts1)
			count++
			totalTime += dur
			avg := time.Duration(int64(totalTime) / int64(count))
			log.Println("\t\t\t\t\t", avg, dur)
			if err != nil {
				if !(err == FrameNotValidError || err.Error() == "no positions found") {
					log.Println(err)
				}
			}
		}
		err = client.Error()
		if err != io.EOF {
			return err
		} else {
			log.Println("Redial Triggered:", err)
		}
	}
}

func RunReaper(sentryWorker SentryWorker, duration time.Duration, skipCooldown bool) {
	if !skipCooldown {
		time.Sleep(duration)
	}
	for {
		nodes, err := sentryWorker.ReapLiveNodes()
		if err != nil {
			log.Println(err)
			continue
		}
		for _, v := range nodes {
			go sentryWorker.Email(v.Callsign, v.LastSeen)
		}
		time.Sleep(1 * time.Second)
	}
}
