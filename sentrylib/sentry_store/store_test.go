package sentry_store_test

import (
	"fmt"
	"github.com/docker/docker/pkg/testutil/assert"
	"github.com/fkautz/sentry/sentrylib"
	"github.com/fkautz/sentry/sentrylib/sentry_store"
	"github.com/fkautz/sentry/sentrylib/sentry_store/sentry_bolt"
	"github.com/fkautz/sentry/sentrylib/sentry_store/sentry_golevel"
	"github.com/fkautz/sentry/sentrylib/sentry_store/sentry_pg"
	"github.com/fkautz/sentry/sentrylib/sentry_store/sentry_rethink"
	"github.com/spf13/viper"
	"gopkg.in/gorethink/gorethink.v3"
	"log"
	"testing"
	"time"
)

var storages []sentry_store.Store

func init() {
	log.SetFlags(log.Flags() | log.Lshortfile)
	bolt, err := sentry_bolt.NewBoltStore("/tmp/test.db")
	if err != nil {
		log.Fatalln(err)
	}

	leveldb, err := sentry_goleveldb.NewGoLevelDB("/tmp/test_lvldb.db")
	if err != nil {
		log.Fatalln(err)
	}

	runtime_viper := viper.New()
	//
	//configTest, err := ioutil.ReadFile("test.json")
	//if err != nil {
	//	log.Fatalln("Unable to open test.json")
	//}
	//err = runtime_viper.ReadConfig(bytes.NewBuffer(configTest))
	//log.Println(bytes.NewBuffer(configTest).String())
	//
	//if err != nil {
	//	log.Fatalln("Unable to open test.json")
	//}
	runtime_viper.SetConfigFile("test.json")
	runtime_viper.SetConfigType("json")
	runtime_viper.ReadRemoteConfig()
	runtime_viper.ReadInConfig()
	cfg := sentrylib.Config{}
	runtime_viper.Unmarshal(&cfg)

	connString := fmt.Sprintf("user=%s password='%s' host=%s dbname=%s sslmode=%s",
		cfg.PostgresConfig.User,
		cfg.PostgresConfig.Password,
		cfg.PostgresConfig.Host,
		cfg.PostgresConfig.DbName,
		cfg.PostgresConfig.SslMode)
	postgresStore, err := sentry_pg.NewPostgresDB(connString)
	if err != nil {
		log.Fatalln(err)
	}

	opts := gorethink.ConnectOpts{}
	rethinkdb, err := sentry_rethink.NewRethinkDB(opts, "test")
	if err != nil {
		log.Fatalln(err)
	}

	storages = make([]sentry_store.Store, 0, 0)
	storages = append(storages, bolt)
	storages = append(storages, leveldb)
	storages = append(storages, postgresStore)
	storages = append(storages, rethinkdb)
}

func TestStore_AddLiveNew(t *testing.T) {
	for _, storage := range storages {
		storage.RemoveLive("FOO", time.Now())
		defer storage.RemoveLive("FOO", time.Now().Add(1*time.Hour))
		ts1 := time.Now()
		time.Sleep(time.Millisecond)
		err := storage.AddLive("FOO")
		time.Sleep(time.Millisecond)
		ts2 := time.Now()
		assert.NilError(t, err)
		ts, ok, err := storage.GetLive("FOO")
		assert.NilError(t, err)
		assert.Equal(t, ok, true)
		assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
	}
}

func TestStore_AddLiveExisting(t *testing.T) {
	for _, storage := range storages {
		defer storage.RemoveLive("FOO", time.Now().Add(1*time.Hour))
		err := storage.AddLive("FOO")
		time.Sleep(time.Millisecond)
		ts1 := time.Now()
		time.Sleep(time.Millisecond)
		err = storage.AddLive("FOO")
		time.Sleep(time.Millisecond)
		ts2 := time.Now()
		assert.NilError(t, err)
		ts, ok, err := storage.GetLive("FOO")
		assert.NilError(t, err)
		assert.Equal(t, ok, true)
		assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
	}
}

func TestStore_GetLiveNoKey(t *testing.T) {
	for _, storage := range storages {
		storage.RemoveLive("NOEXIST", time.Now())
		_, ok, err := storage.GetLive("NOEXIST")
		assert.Equal(t, ok, false)
		assert.NilError(t, err)
	}
}

func TestStore_RemoveLive(t *testing.T) {
	for _, storage := range storages {
		err := storage.RemoveLive("FOO", time.Now())
		assert.NilError(t, err)

		err = storage.AddLive("FOO")
		assert.NilError(t, err)
		_, ok, err := storage.GetLive("FOO")
		assert.Equal(t, ok, true)
		assert.NilError(t, err)

		err = storage.RemoveLive("FOO", time.Now())
		assert.NilError(t, err)

		_, ok, err = storage.GetLive("FOO")
		assert.Equal(t, ok, false)
		assert.NilError(t, err)
	}
}

func TestStore_ListLive(t *testing.T) {
	for _, storage := range storages {
		storage.RemoveLive("FOO1", time.Now())
		storage.RemoveLive("FOO2", time.Now())
		storage.RemoveLive("FOO3", time.Now())
		storage.RemoveLive("FOO4", time.Now())
		storage.RemoveLive("FOO5", time.Now())
		defer storage.RemoveLive("FOO1", time.Now().Add(1*time.Hour))
		defer storage.RemoveLive("FOO2", time.Now().Add(1*time.Hour))
		defer storage.RemoveLive("FOO3", time.Now().Add(1*time.Hour))
		defer storage.RemoveLive("FOO4", time.Now().Add(1*time.Hour))
		defer storage.RemoveLive("FOO5", time.Now().Add(1*time.Hour))

		startTs := time.Now()

		list, err := storage.ListLive(time.Now())
		assert.NilError(t, err)
		assert.DeepEqual(t, list, make([]sentry_store.CallsignTime, 0))

		storage.AddLive("FOO1")
		storage.AddLive("FOO2")
		storage.AddLive("FOO3")
		ts := time.Now()

		list, err = storage.ListLive(ts)
		assert.NilError(t, err)
		assert.Equal(t, len(list), 3)
		assert.Equal(t, list[0].Callsign, "FOO1")
		assert.Equal(t, list[1].Callsign, "FOO2")
		assert.Equal(t, list[2].Callsign, "FOO3")

		storage.AddLive("FOO4")
		storage.AddLive("FOO5")

		list, err = storage.ListLive(ts)
		assert.Equal(t, len(list), 3)
		assert.Equal(t, list[0].Callsign, "FOO1")
		assert.Equal(t, list[1].Callsign, "FOO2")
		assert.Equal(t, list[2].Callsign, "FOO3")

		list, err = storage.ListLive(time.Now())
		assert.Equal(t, len(list), 5)
		assert.Equal(t, list[0].Callsign, "FOO1")
		assert.Equal(t, list[1].Callsign, "FOO2")
		assert.Equal(t, list[2].Callsign, "FOO3")
		assert.Equal(t, list[3].Callsign, "FOO4")
		assert.Equal(t, list[4].Callsign, "FOO5")

		lastSeen := startTs
		for _, v := range list {
			assert.Equal(t, lastSeen.Before(v.LastSeen), true)
			lastSeen = v.LastSeen
		}
		lastSeen.Before(time.Now())
	}
}

func TestStore_CountLive(t *testing.T) {
	for _, storage := range storages {
		storage.RemoveLive("FOO1", time.Now())
		storage.RemoveLive("FOO2", time.Now())
		storage.RemoveLive("FOO3", time.Now())
		storage.RemoveLive("FOO4", time.Now())
		storage.RemoveLive("FOO5", time.Now())
		defer storage.RemoveLive("FOO1", time.Now().Add(1*time.Hour))
		defer storage.RemoveLive("FOO2", time.Now().Add(1*time.Hour))
		defer storage.RemoveLive("FOO3", time.Now().Add(1*time.Hour))
		defer storage.RemoveLive("FOO4", time.Now().Add(1*time.Hour))
		defer storage.RemoveLive("FOO5", time.Now().Add(1*time.Hour))

		count, err := storage.CountLive()
		assert.NilError(t, err)
		assert.Equal(t, count, 0)

		err = storage.AddLive("FOO1")
		assert.NilError(t, err)
		count, err = storage.CountLive()
		assert.NilError(t, err)
		assert.Equal(t, count, 1)

		err = storage.AddLive("FOO2")
		assert.NilError(t, err)
		count, err = storage.CountLive()
		assert.NilError(t, err)
		assert.Equal(t, count, 2)

		err = storage.AddLive("FOO3")
		assert.NilError(t, err)
		count, err = storage.CountLive()
		assert.NilError(t, err)
		assert.Equal(t, count, 3)

		err = storage.RemoveLive("FOO1", time.Now())
		assert.NilError(t, err)
		count, err = storage.CountLive()
		assert.NilError(t, err)
		assert.Equal(t, count, 2)

		err = storage.RemoveLive("FOO2", time.Now())
		assert.NilError(t, err)
		count, err = storage.CountLive()
		assert.NilError(t, err)
		assert.Equal(t, count, 1)

		err = storage.RemoveLive("FOO3", time.Now())
		assert.NilError(t, err)
		count, err = storage.CountLive()
		assert.NilError(t, err)
		assert.Equal(t, count, 0)
	}
}

func TestStore_AddDeadNew(t *testing.T) {
	for _, storage := range storages {
		storage.RemoveDead("FOO")
		defer storage.RemoveDead("FOO")
		ts1 := time.Now()
		time.Sleep(time.Millisecond)
		err := storage.AddDead("FOO", time.Now())
		time.Sleep(time.Millisecond)
		ts2 := time.Now()
		assert.NilError(t, err)
		ts, ok, err := storage.GetDead("FOO")
		assert.NilError(t, err)
		assert.Equal(t, ok, true)
		assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
	}
}

func TestStore_AddDeadExisting(t *testing.T) {
	for _, storage := range storages {
		defer storage.RemoveDead("FOO")
		err := storage.AddDead("FOO", time.Now())
		ts1 := time.Now()
		time.Sleep(time.Millisecond)
		err = storage.AddDead("FOO", time.Now())
		time.Sleep(time.Millisecond)
		ts2 := time.Now()
		assert.NilError(t, err)
		ts, ok, err := storage.GetDead("FOO")
		assert.NilError(t, err)
		assert.Equal(t, ok, true)
		assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
	}
}

func TestStore_GetDeadNoKey(t *testing.T) {
	for _, storage := range storages {
		storage.RemoveDead("NOEXIST")
		_, ok, err := storage.GetDead("NOEXIST")
		assert.Equal(t, ok, false)
		assert.NilError(t, err)
	}
}

func TestStore_RemoveDead(t *testing.T) {
	for _, storage := range storages {
		err := storage.RemoveDead("FOO")
		assert.NilError(t, err)

		err = storage.AddDead("FOO", time.Now())
		assert.NilError(t, err)
		_, ok, err := storage.GetDead("FOO")
		assert.Equal(t, ok, true)
		assert.NilError(t, err)

		err = storage.RemoveDead("FOO")
		assert.NilError(t, err)

		_, ok, err = storage.GetDead("FOO")
		assert.Equal(t, ok, false)
		assert.NilError(t, err)
	}
}

func TestStore_ListDead(t *testing.T) {
	for _, storage := range storages {
		storage.RemoveDead("FOO1")
		storage.RemoveDead("FOO2")
		storage.RemoveDead("FOO3")
		storage.RemoveDead("FOO4")
		storage.RemoveDead("FOO5")
		defer storage.RemoveDead("FOO1")
		defer storage.RemoveDead("FOO2")
		defer storage.RemoveDead("FOO3")
		defer storage.RemoveDead("FOO4")
		defer storage.RemoveDead("FOO5")

		startTs := time.Now()

		list, err := storage.ListDead()
		assert.NilError(t, err)
		assert.DeepEqual(t, list, make([]sentry_store.CallsignTime, 0))

		storage.AddDead("FOO1", time.Now())
		storage.AddDead("FOO2", time.Now())
		storage.AddDead("FOO3", time.Now())

		list, err = storage.ListDead()
		assert.NilError(t, err)
		assert.Equal(t, len(list), 3)
		assert.Equal(t, list[0].Callsign, "FOO1")
		assert.Equal(t, list[1].Callsign, "FOO2")
		assert.Equal(t, list[2].Callsign, "FOO3")

		storage.AddDead("FOO4", time.Now())
		storage.AddDead("FOO5", time.Now())

		list, err = storage.ListDead()
		assert.Equal(t, len(list), 5)
		assert.Equal(t, list[0].Callsign, "FOO1")
		assert.Equal(t, list[1].Callsign, "FOO2")
		assert.Equal(t, list[2].Callsign, "FOO3")
		assert.Equal(t, list[3].Callsign, "FOO4")
		assert.Equal(t, list[4].Callsign, "FOO5")

		lastSeen := startTs
		for _, v := range list {
			assert.Equal(t, lastSeen.Before(v.LastSeen), true)
			lastSeen = v.LastSeen
		}
		lastSeen.Before(time.Now())
	}
}

func TestStore_CountDead(t *testing.T) {
	for _, storage := range storages {
		storage.RemoveDead("FOO1")
		storage.RemoveDead("FOO2")
		storage.RemoveDead("FOO3")
		storage.RemoveDead("FOO4")
		storage.RemoveDead("FOO5")
		defer storage.RemoveDead("FOO1")
		defer storage.RemoveDead("FOO2")
		defer storage.RemoveDead("FOO3")
		defer storage.RemoveDead("FOO4")
		defer storage.RemoveDead("FOO5")

		count, err := storage.CountDead()
		assert.NilError(t, err)
		assert.Equal(t, count, 0)

		err = storage.AddDead("FOO1", time.Now())
		assert.NilError(t, err)
		count, err = storage.CountDead()
		assert.NilError(t, err)
		assert.Equal(t, count, 1)

		err = storage.AddDead("FOO2", time.Now())
		assert.NilError(t, err)
		count, err = storage.CountDead()
		assert.NilError(t, err)
		assert.Equal(t, count, 2)

		err = storage.AddDead("FOO3", time.Now())
		assert.NilError(t, err)
		count, err = storage.CountDead()
		assert.NilError(t, err)
		assert.Equal(t, count, 3)

		err = storage.RemoveDead("FOO1")
		assert.NilError(t, err)
		count, err = storage.CountDead()
		assert.NilError(t, err)
		assert.Equal(t, count, 2)

		err = storage.RemoveDead("FOO2")
		assert.NilError(t, err)
		count, err = storage.CountDead()
		assert.NilError(t, err)
		assert.Equal(t, count, 1)

		err = storage.RemoveDead("FOO3")
		assert.NilError(t, err)
		count, err = storage.CountDead()
		assert.NilError(t, err)
		assert.Equal(t, count, 0)
	}
}

func TestStore_AddEmail(t *testing.T) {
	for _, storage := range storages {
		err := storage.RemoveEmail("foo")
		assert.NilError(t, err)

		// duplicate to test when definitely empty
		err = storage.RemoveEmail("foo")
		assert.NilError(t, err)

		email, ok, err := storage.GetEmail("foo")
		assert.Equal(t, email, "")
		assert.Equal(t, ok, false)
		assert.NilError(t, err)

		err = storage.AddEmail("foo", "bar")
		assert.NilError(t, err)

		email, ok, err = storage.GetEmail("foo")
		assert.Equal(t, email, "bar")
		assert.Equal(t, ok, true)
		assert.NilError(t, err)

		err = storage.AddEmail("foo", "bar,jitsu")
		assert.NilError(t, err)

		email, ok, err = storage.GetEmail("foo")
		assert.Equal(t, email, "bar,jitsu")
		assert.Equal(t, ok, true)
		assert.NilError(t, err)

		err = storage.RemoveEmail("foo")
		assert.NilError(t, err)

		email, ok, err = storage.GetEmail("foo")
		assert.Equal(t, email, "")
		assert.Equal(t, ok, false)
		assert.NilError(t, err)
	}
}

func TestStore_ListEmail(t *testing.T) {
	for _, storage := range storages {
		storage.RemoveEmail("foo1")
		storage.RemoveEmail("foo2")
		storage.RemoveEmail("foo3")
		storage.RemoveEmail("foo4")
		storage.RemoveEmail("foo5")
		defer storage.RemoveEmail("foo1")
		defer storage.RemoveEmail("foo2")
		defer storage.RemoveEmail("foo3")
		defer storage.RemoveEmail("foo4")
		defer storage.RemoveEmail("foo5")

		list, err := storage.ListEmail()
		assert.Equal(t, len(list), 0)
		assert.NilError(t, err)

		storage.AddEmail("foo1", "bar1")
		storage.AddEmail("foo2", "bar2")
		storage.AddEmail("foo3", "bar3")
		storage.AddEmail("foo4", "bar4")
		storage.AddEmail("foo5", "bar5")

		expectedList := make([]sentry_store.CallsignEmail, 5, 5)
		expectedList[0] = sentry_store.CallsignEmail{"foo1", "bar1"}
		expectedList[1] = sentry_store.CallsignEmail{"foo2", "bar2"}
		expectedList[2] = sentry_store.CallsignEmail{"foo3", "bar3"}
		expectedList[3] = sentry_store.CallsignEmail{"foo4", "bar4"}
		expectedList[4] = sentry_store.CallsignEmail{"foo5", "bar5"}

		list, err = storage.ListEmail()
		assert.DeepEqual(t, list, expectedList)
		assert.NilError(t, err)

		storage.RemoveEmail("foo4")
		storage.RemoveEmail("foo5")

		expectedList = expectedList[0:3]

		list, err = storage.ListEmail()
		assert.DeepEqual(t, list, expectedList)
		assert.NilError(t, err)

		storage.RemoveEmail("foo1")
		storage.RemoveEmail("foo2")
		storage.RemoveEmail("foo3")

		list, err = storage.ListEmail()
		assert.Equal(t, len(list), 0)
		assert.NilError(t, err)
	}
}
