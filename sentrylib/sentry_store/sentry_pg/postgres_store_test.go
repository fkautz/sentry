package sentry_pg

import (
	"github.com/docker/docker/pkg/testutil/assert"
	"github.com/fkautz/sentry/sentrylib/sentry_store"
	"log"
	"testing"
	"time"
)

var postgresStore sentry_store.Store

func init() {
	var err error
	postgresStore, err = NewPostgresDB("sentrytest")
	if err != nil {
		log.Println(err)
	}

	log.SetFlags(log.Flags() | log.Lshortfile)
}

func TestPostgresStore_AddLiveNew(t *testing.T) {
	postgresStore.RemoveLive("FOO", time.Now())
	defer postgresStore.RemoveLive("FOO", time.Now().Add(1*time.Hour))
	ts1 := time.Now()
	time.Sleep(1 * time.Millisecond)
	err := postgresStore.AddLive("FOO")
	time.Sleep(1 * time.Millisecond)
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := postgresStore.GetLive("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestPostgresStore_AddLiveExisting(t *testing.T) {
	defer postgresStore.RemoveLive("FOO", time.Now().Add(1*time.Hour))
	err := postgresStore.AddLive("FOO")
	time.Sleep(1 * time.Millisecond)
	ts1 := time.Now()
	time.Sleep(1 * time.Millisecond)
	err = postgresStore.AddLive("FOO")
	time.Sleep(1 * time.Millisecond)
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := postgresStore.GetLive("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestPostgresStore_GetLiveNoKey(t *testing.T) {
	err := postgresStore.RemoveLive("NOEXIST", time.Now())
	assert.NilError(t, err)
	_, ok, err := postgresStore.GetLive("NOEXIST")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestPostgresStore_RemoveLive(t *testing.T) {
	err := postgresStore.RemoveLive("FOO", time.Now())
	assert.NilError(t, err)

	err = postgresStore.AddLive("FOO")
	assert.NilError(t, err)

	_, ok, err := postgresStore.GetLive("FOO")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = postgresStore.RemoveLive("FOO", time.Now())
	assert.NilError(t, err)

	_, ok, err = postgresStore.GetLive("FOO")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestPostgresStore_ListLive(t *testing.T) {
	postgresStore.RemoveLive("FOO1", time.Now())
	postgresStore.RemoveLive("FOO2", time.Now())
	postgresStore.RemoveLive("FOO3", time.Now())
	postgresStore.RemoveLive("FOO4", time.Now())
	postgresStore.RemoveLive("FOO5", time.Now())
	defer postgresStore.RemoveLive("FOO1", time.Now().Add(1*time.Hour))
	defer postgresStore.RemoveLive("FOO2", time.Now().Add(1*time.Hour))
	defer postgresStore.RemoveLive("FOO3", time.Now().Add(1*time.Hour))
	defer postgresStore.RemoveLive("FOO4", time.Now().Add(1*time.Hour))
	defer postgresStore.RemoveLive("FOO5", time.Now().Add(1*time.Hour))

	startTs := time.Now()

	list, err := postgresStore.ListLive(time.Now())
	assert.NilError(t, err)
	assert.DeepEqual(t, list, make([]sentry_store.CallsignTime, 0))

	postgresStore.AddLive("FOO1")
	postgresStore.AddLive("FOO2")
	postgresStore.AddLive("FOO3")
	ts := time.Now()

	list, err = postgresStore.ListLive(ts)
	assert.NilError(t, err)
	assert.Equal(t, len(list), 3)
	assert.Equal(t, list[0].Callsign, "FOO1")
	assert.Equal(t, list[1].Callsign, "FOO2")
	assert.Equal(t, list[2].Callsign, "FOO3")

	postgresStore.AddLive("FOO4")
	postgresStore.AddLive("FOO5")

	list, err = postgresStore.ListLive(ts)
	assert.Equal(t, len(list), 3)
	assert.Equal(t, list[0].Callsign, "FOO1")
	assert.Equal(t, list[1].Callsign, "FOO2")
	assert.Equal(t, list[2].Callsign, "FOO3")

	list, err = postgresStore.ListLive(time.Now())
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

func TestPostgresStore_CountLive(t *testing.T) {
	postgresStore.RemoveLive("FOO1", time.Now())
	postgresStore.RemoveLive("FOO2", time.Now())
	postgresStore.RemoveLive("FOO3", time.Now())
	postgresStore.RemoveLive("FOO4", time.Now())
	postgresStore.RemoveLive("FOO5", time.Now())
	defer postgresStore.RemoveLive("FOO1", time.Now().Add(1*time.Hour))
	defer postgresStore.RemoveLive("FOO2", time.Now().Add(1*time.Hour))
	defer postgresStore.RemoveLive("FOO3", time.Now().Add(1*time.Hour))
	defer postgresStore.RemoveLive("FOO4", time.Now().Add(1*time.Hour))
	defer postgresStore.RemoveLive("FOO5", time.Now().Add(1*time.Hour))

	count, err := postgresStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)

	err = postgresStore.AddLive("FOO1")
	assert.NilError(t, err)
	count, err = postgresStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = postgresStore.AddLive("FOO2")
	assert.NilError(t, err)
	count, err = postgresStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = postgresStore.AddLive("FOO3")
	assert.NilError(t, err)
	count, err = postgresStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 3)

	err = postgresStore.RemoveLive("FOO1", time.Now())
	assert.NilError(t, err)
	count, err = postgresStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = postgresStore.RemoveLive("FOO2", time.Now())
	assert.NilError(t, err)
	count, err = postgresStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = postgresStore.RemoveLive("FOO3", time.Now())
	assert.NilError(t, err)
	count, err = postgresStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)
}

func TestPostgresStore_AddDeadNew(t *testing.T) {
	postgresStore.RemoveDead("FOO")
	defer postgresStore.RemoveDead("FOO")
	ts1 := time.Now()
	time.Sleep(1 * time.Millisecond)
	err := postgresStore.AddDead("FOO", time.Now())
	time.Sleep(1 * time.Millisecond)
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := postgresStore.GetDead("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestPostgresStore_AddDeadExisting(t *testing.T) {
	defer postgresStore.RemoveDead("FOO")
	err := postgresStore.AddDead("FOO", time.Now())
	time.Sleep(1 * time.Millisecond)
	ts1 := time.Now()
	time.Sleep(1 * time.Millisecond)
	err = postgresStore.AddDead("FOO", time.Now())
	time.Sleep(1 * time.Millisecond)
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := postgresStore.GetDead("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestPostgresStore_GetDeadNoKey(t *testing.T) {
	postgresStore.RemoveDead("NOEXIST")
	_, ok, err := postgresStore.GetDead("NOEXIST")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestPostgresStore_RemoveDead(t *testing.T) {
	err := postgresStore.RemoveDead("FOO")
	assert.NilError(t, err)

	err = postgresStore.AddDead("FOO", time.Now())
	assert.NilError(t, err)
	_, ok, err := postgresStore.GetDead("FOO")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = postgresStore.RemoveDead("FOO")
	assert.NilError(t, err)

	_, ok, err = postgresStore.GetDead("FOO")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestPostgresStore_ListDead(t *testing.T) {
	postgresStore.RemoveDead("FOO1")
	postgresStore.RemoveDead("FOO2")
	postgresStore.RemoveDead("FOO3")
	postgresStore.RemoveDead("FOO4")
	postgresStore.RemoveDead("FOO5")
	defer postgresStore.RemoveDead("FOO1")
	defer postgresStore.RemoveDead("FOO2")
	defer postgresStore.RemoveDead("FOO3")
	defer postgresStore.RemoveDead("FOO4")
	defer postgresStore.RemoveDead("FOO5")

	startTs := time.Now()

	list, err := postgresStore.ListDead()
	assert.NilError(t, err)
	assert.DeepEqual(t, list, make([]sentry_store.CallsignTime, 0))

	postgresStore.AddDead("FOO1", time.Now())
	postgresStore.AddDead("FOO2", time.Now())
	postgresStore.AddDead("FOO3", time.Now())

	list, err = postgresStore.ListDead()
	assert.NilError(t, err)
	assert.Equal(t, len(list), 3)
	assert.Equal(t, list[0].Callsign, "FOO1")
	assert.Equal(t, list[1].Callsign, "FOO2")
	assert.Equal(t, list[2].Callsign, "FOO3")

	postgresStore.AddDead("FOO4", time.Now())
	postgresStore.AddDead("FOO5", time.Now())

	list, err = postgresStore.ListDead()
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

func TestPostgresStore_CountDead(t *testing.T) {
	postgresStore.RemoveDead("FOO1")
	postgresStore.RemoveDead("FOO2")
	postgresStore.RemoveDead("FOO3")
	postgresStore.RemoveDead("FOO4")
	postgresStore.RemoveDead("FOO5")
	defer postgresStore.RemoveDead("FOO1")
	defer postgresStore.RemoveDead("FOO2")
	defer postgresStore.RemoveDead("FOO3")
	defer postgresStore.RemoveDead("FOO4")
	defer postgresStore.RemoveDead("FOO5")

	count, err := postgresStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)

	err = postgresStore.AddDead("FOO1", time.Now())
	assert.NilError(t, err)
	count, err = postgresStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = postgresStore.AddDead("FOO2", time.Now())
	assert.NilError(t, err)
	count, err = postgresStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = postgresStore.AddDead("FOO3", time.Now())
	assert.NilError(t, err)
	count, err = postgresStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 3)

	err = postgresStore.RemoveDead("FOO1")
	assert.NilError(t, err)
	count, err = postgresStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = postgresStore.RemoveDead("FOO2")
	assert.NilError(t, err)
	count, err = postgresStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = postgresStore.RemoveDead("FOO3")
	assert.NilError(t, err)
	count, err = postgresStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)
}

func TestPostgresStore_AddEmail(t *testing.T) {
	err := postgresStore.RemoveEmail("foo")
	assert.NilError(t, err)

	// duplicate to test when definitely empty
	err = postgresStore.RemoveEmail("foo")
	assert.NilError(t, err)

	email, ok, err := postgresStore.GetEmail("foo")
	assert.Equal(t, email, "")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)

	err = postgresStore.AddEmail("foo", "bar")
	assert.NilError(t, err)

	email, ok, err = postgresStore.GetEmail("foo")
	assert.Equal(t, email, "bar")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = postgresStore.AddEmail("foo", "bar,jitsu")
	assert.NilError(t, err)

	email, ok, err = postgresStore.GetEmail("foo")
	assert.Equal(t, email, "bar,jitsu")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = postgresStore.RemoveEmail("foo")
	assert.NilError(t, err)

	email, ok, err = postgresStore.GetEmail("foo")
	assert.Equal(t, email, "")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestPostgresStore_ListEmail(t *testing.T) {
	postgresStore.RemoveEmail("foo1")
	postgresStore.RemoveEmail("foo2")
	postgresStore.RemoveEmail("foo3")
	postgresStore.RemoveEmail("foo4")
	postgresStore.RemoveEmail("foo5")
	defer postgresStore.RemoveEmail("foo1")
	defer postgresStore.RemoveEmail("foo2")
	defer postgresStore.RemoveEmail("foo3")
	defer postgresStore.RemoveEmail("foo4")
	defer postgresStore.RemoveEmail("foo5")

	list, err := postgresStore.ListEmail()
	assert.Equal(t, len(list), 0)
	assert.NilError(t, err)

	postgresStore.AddEmail("foo1", "bar1")
	postgresStore.AddEmail("foo2", "bar2")
	postgresStore.AddEmail("foo3", "bar3")
	postgresStore.AddEmail("foo4", "bar4")
	postgresStore.AddEmail("foo5", "bar5")

	expectedList := make([]sentry_store.CallsignEmail, 5, 5)
	expectedList[0] = sentry_store.CallsignEmail{"foo1", "bar1"}
	expectedList[1] = sentry_store.CallsignEmail{"foo2", "bar2"}
	expectedList[2] = sentry_store.CallsignEmail{"foo3", "bar3"}
	expectedList[3] = sentry_store.CallsignEmail{"foo4", "bar4"}
	expectedList[4] = sentry_store.CallsignEmail{"foo5", "bar5"}

	list, err = postgresStore.ListEmail()
	assert.DeepEqual(t, list, expectedList)
	assert.NilError(t, err)

	postgresStore.RemoveEmail("foo4")
	postgresStore.RemoveEmail("foo5")

	expectedList = expectedList[0:3]

	list, err = postgresStore.ListEmail()
	assert.DeepEqual(t, list, expectedList)
	assert.NilError(t, err)

	postgresStore.RemoveEmail("foo1")
	postgresStore.RemoveEmail("foo2")
	postgresStore.RemoveEmail("foo3")

	list, err = postgresStore.ListEmail()
	assert.Equal(t, len(list), 0)
	assert.NilError(t, err)
}
