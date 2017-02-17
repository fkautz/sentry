package sentrylib

import (
	"errors"
	"fmt"
	"github.com/fkautz/sentry/sentrylib/sentry_store"
	"github.com/fkautz/sentry/sentrylib/sentry_store/sentry_bolt"
	"github.com/fkautz/sentry/sentrylib/sentry_store/sentry_golevel"
	"github.com/fkautz/sentry/sentrylib/sentry_store/sentry_pg"
	"github.com/fkautz/sentry/sentrylib/sentry_store/sentry_rethink"
	"gopkg.in/gorethink/gorethink.v3"
	"gopkg.in/yaml.v2"
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

	mout, _ := yaml.Marshal(server.config)
	log.Println(string(mout))

	dbcount := 0
	var store sentry_store.Store
	var err error
	if server.config.BoltConfig != nil {
		dbcount++
		store, err = sentry_bolt.NewBoltStore("sentry.db")
	}
	if server.config.PostgresConfig != nil {
		dbcount++
		connString := ""
		if server.config.PostgresConfig.ConnString != "" {
			connString = server.config.PostgresConfig.ConnString
		} else {
			user := server.config.PostgresConfig.User
			password := server.config.PostgresConfig.Password
			host := server.config.PostgresConfig.Host
			dbname := server.config.PostgresConfig.DbName
			sslmode := server.config.PostgresConfig.SslMode
			connString = fmt.Sprintf("user=%s password='%s' host=%s dbname=%s sslmode=%s", user, password, host, dbname, sslmode)
		}
		log.Println(connString)
		store, err = sentry_pg.NewPostgresDB(connString)
	}
	if server.config.GoLevelDBConfig != nil {
		dbcount++
		store, err = sentry_goleveldb.NewGoLevelDB(server.config.GoLevelDBConfig.File)
	}
	if server.config.RethinkDBConfig != nil {
		dbcount++
		opts := gorethink.ConnectOpts{}
		if server.config.RethinkDBConfig.Address != "" {
			opts.Address = server.config.RethinkDBConfig.Address
		}
		if server.config.RethinkDBConfig.Username != "" {
			opts.Username = server.config.RethinkDBConfig.Username
		}
		if server.config.RethinkDBConfig.Password != "" {
			opts.Password = server.config.RethinkDBConfig.Password
		}
		store, err = sentry_rethink.NewRethinkDB(opts, server.config.RethinkDBConfig.Database)
	}
	if dbcount != 1 {
		log.Fatalln("There should one database configured")
	}

	//store, err := sentry_rethink.NewRethinkDB("localhost", "dev")
	//store, err := sentry_goleveldb.NewGoLevelDB("level.db")
	//store, err := sentry_pg.NewPostgresDB("sentry")
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
