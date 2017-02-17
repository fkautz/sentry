package sentry_rethink

import (
	"github.com/docker/docker/pkg/testutil/assert"
	"github.com/fkautz/sentry/sentrylib/sentry_store"
	"log"
	"testing"
	"time"
)

var rethinkStore sentry_store.Store

func init() {
	var err error
	rethinkStore, err = NewRethinkDB("localhost", "test")
	if err != nil {
		log.Println(err)
	}

	log.SetFlags(log.Flags() | log.Lshortfile)
}

func TestRethinkDBStore_AddLiveNew(t *testing.T) {
	rethinkStore.RemoveLive("FOO", time.Now())
	defer rethinkStore.RemoveLive("FOO", time.Now().Add(1*time.Hour))
	ts1 := time.Now()
	time.Sleep(1 * time.Millisecond)
	err := rethinkStore.AddLive("FOO")
	time.Sleep(1 * time.Millisecond)
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := rethinkStore.GetLive("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestRethinkDBStore_AddLiveExisting(t *testing.T) {
	defer rethinkStore.RemoveLive("FOO", time.Now().Add(1*time.Hour))
	err := rethinkStore.AddLive("FOO")
	time.Sleep(1 * time.Millisecond)
	ts1 := time.Now()
	time.Sleep(1 * time.Millisecond)
	err = rethinkStore.AddLive("FOO")
	time.Sleep(1 * time.Millisecond)
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := rethinkStore.GetLive("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestRethinkDBStore_GetLiveNoKey(t *testing.T) {
	rethinkStore.RemoveLive("NOEXIST", time.Now())
	_, ok, err := rethinkStore.GetLive("NOEXIST")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestRethinkDBStore_RemoveLive(t *testing.T) {
	err := rethinkStore.RemoveLive("FOO", time.Now())
	assert.NilError(t, err)

	err = rethinkStore.AddLive("FOO")
	assert.NilError(t, err)
	_, ok, err := rethinkStore.GetLive("FOO")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = rethinkStore.RemoveLive("FOO", time.Now())
	assert.NilError(t, err)

	_, ok, err = rethinkStore.GetLive("FOO")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestRethinkDBStore_ListLive(t *testing.T) {
	rethinkStore.RemoveLive("FOO1", time.Now())
	rethinkStore.RemoveLive("FOO2", time.Now())
	rethinkStore.RemoveLive("FOO3", time.Now())
	rethinkStore.RemoveLive("FOO4", time.Now())
	rethinkStore.RemoveLive("FOO5", time.Now())
	defer rethinkStore.RemoveLive("FOO1", time.Now().Add(1*time.Hour))
	defer rethinkStore.RemoveLive("FOO2", time.Now().Add(1*time.Hour))
	defer rethinkStore.RemoveLive("FOO3", time.Now().Add(1*time.Hour))
	defer rethinkStore.RemoveLive("FOO4", time.Now().Add(1*time.Hour))
	defer rethinkStore.RemoveLive("FOO5", time.Now().Add(1*time.Hour))

	startTs := time.Now()

	list, err := rethinkStore.ListLive(time.Now())
	assert.NilError(t, err)
	assert.DeepEqual(t, list, make([]sentry_store.CallsignTime, 0))

	rethinkStore.AddLive("FOO1")
	rethinkStore.AddLive("FOO2")
	rethinkStore.AddLive("FOO3")
	ts := time.Now()

	list, err = rethinkStore.ListLive(ts)
	assert.NilError(t, err)
	assert.Equal(t, len(list), 3)
	assert.Equal(t, list[0].Callsign, "FOO1")
	assert.Equal(t, list[1].Callsign, "FOO2")
	assert.Equal(t, list[2].Callsign, "FOO3")

	rethinkStore.AddLive("FOO4")
	rethinkStore.AddLive("FOO5")

	list, err = rethinkStore.ListLive(ts)
	assert.Equal(t, len(list), 3)
	assert.Equal(t, list[0].Callsign, "FOO1")
	assert.Equal(t, list[1].Callsign, "FOO2")
	assert.Equal(t, list[2].Callsign, "FOO3")

	list, err = rethinkStore.ListLive(time.Now())
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

func TestRethinkDBStore_CountLive(t *testing.T) {
	rethinkStore.RemoveLive("FOO1", time.Now())
	rethinkStore.RemoveLive("FOO2", time.Now())
	rethinkStore.RemoveLive("FOO3", time.Now())
	rethinkStore.RemoveLive("FOO4", time.Now())
	rethinkStore.RemoveLive("FOO5", time.Now())
	defer rethinkStore.RemoveLive("FOO1", time.Now().Add(1*time.Hour))
	defer rethinkStore.RemoveLive("FOO2", time.Now().Add(1*time.Hour))
	defer rethinkStore.RemoveLive("FOO3", time.Now().Add(1*time.Hour))
	defer rethinkStore.RemoveLive("FOO4", time.Now().Add(1*time.Hour))
	defer rethinkStore.RemoveLive("FOO5", time.Now().Add(1*time.Hour))

	count, err := rethinkStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)

	err = rethinkStore.AddLive("FOO1")
	assert.NilError(t, err)
	count, err = rethinkStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = rethinkStore.AddLive("FOO2")
	assert.NilError(t, err)
	count, err = rethinkStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = rethinkStore.AddLive("FOO3")
	assert.NilError(t, err)
	count, err = rethinkStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 3)

	err = rethinkStore.RemoveLive("FOO1", time.Now())
	assert.NilError(t, err)
	count, err = rethinkStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = rethinkStore.RemoveLive("FOO2", time.Now())
	assert.NilError(t, err)
	count, err = rethinkStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = rethinkStore.RemoveLive("FOO3", time.Now())
	assert.NilError(t, err)
	count, err = rethinkStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)
}

func TestRethinkDBStore_AddDeadNew(t *testing.T) {
	rethinkStore.RemoveDead("FOO")
	defer rethinkStore.RemoveDead("FOO")
	ts1 := time.Now()
	time.Sleep(1 * time.Millisecond)
	err := rethinkStore.AddDead("FOO", time.Now())
	time.Sleep(1 * time.Millisecond)
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := rethinkStore.GetDead("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestRethinkDBStore_AddDeadExisting(t *testing.T) {
	defer rethinkStore.RemoveDead("FOO")
	err := rethinkStore.AddDead("FOO", time.Now())
	time.Sleep(1 * time.Millisecond)
	ts1 := time.Now()
	time.Sleep(1 * time.Millisecond)
	err = rethinkStore.AddDead("FOO", time.Now())
	time.Sleep(1 * time.Millisecond)
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := rethinkStore.GetDead("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestRethinkDBStore_GetDeadNoKey(t *testing.T) {
	rethinkStore.RemoveDead("NOEXIST")
	_, ok, err := rethinkStore.GetDead("NOEXIST")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestRethinkDBStore_RemoveDead(t *testing.T) {
	err := rethinkStore.RemoveDead("FOO")
	assert.NilError(t, err)

	err = rethinkStore.AddDead("FOO", time.Now())
	assert.NilError(t, err)
	_, ok, err := rethinkStore.GetDead("FOO")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = rethinkStore.RemoveDead("FOO")
	assert.NilError(t, err)

	_, ok, err = rethinkStore.GetDead("FOO")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestRethinkDBStore_ListDead(t *testing.T) {
	rethinkStore.RemoveDead("FOO1")
	rethinkStore.RemoveDead("FOO2")
	rethinkStore.RemoveDead("FOO3")
	rethinkStore.RemoveDead("FOO4")
	rethinkStore.RemoveDead("FOO5")
	defer rethinkStore.RemoveDead("FOO1")
	defer rethinkStore.RemoveDead("FOO2")
	defer rethinkStore.RemoveDead("FOO3")
	defer rethinkStore.RemoveDead("FOO4")
	defer rethinkStore.RemoveDead("FOO5")

	startTs := time.Now()

	list, err := rethinkStore.ListDead()
	assert.NilError(t, err)
	assert.DeepEqual(t, list, make([]sentry_store.CallsignTime, 0))

	rethinkStore.AddDead("FOO1", time.Now())
	rethinkStore.AddDead("FOO2", time.Now())
	rethinkStore.AddDead("FOO3", time.Now())

	list, err = rethinkStore.ListDead()
	assert.NilError(t, err)
	assert.Equal(t, len(list), 3)
	assert.Equal(t, list[0].Callsign, "FOO1")
	assert.Equal(t, list[1].Callsign, "FOO2")
	assert.Equal(t, list[2].Callsign, "FOO3")

	rethinkStore.AddDead("FOO4", time.Now())
	rethinkStore.AddDead("FOO5", time.Now())

	list, err = rethinkStore.ListDead()
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

func TestRethinkDBStore_CountDead(t *testing.T) {
	rethinkStore.RemoveDead("FOO1")
	rethinkStore.RemoveDead("FOO2")
	rethinkStore.RemoveDead("FOO3")
	rethinkStore.RemoveDead("FOO4")
	rethinkStore.RemoveDead("FOO5")
	defer rethinkStore.RemoveDead("FOO1")
	defer rethinkStore.RemoveDead("FOO2")
	defer rethinkStore.RemoveDead("FOO3")
	defer rethinkStore.RemoveDead("FOO4")
	defer rethinkStore.RemoveDead("FOO5")

	count, err := rethinkStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)

	err = rethinkStore.AddDead("FOO1", time.Now())
	assert.NilError(t, err)
	count, err = rethinkStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = rethinkStore.AddDead("FOO2", time.Now())
	assert.NilError(t, err)
	count, err = rethinkStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = rethinkStore.AddDead("FOO3", time.Now())
	assert.NilError(t, err)
	count, err = rethinkStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 3)

	err = rethinkStore.RemoveDead("FOO1")
	assert.NilError(t, err)
	count, err = rethinkStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = rethinkStore.RemoveDead("FOO2")
	assert.NilError(t, err)
	count, err = rethinkStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = rethinkStore.RemoveDead("FOO3")
	assert.NilError(t, err)
	count, err = rethinkStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)
}

func TestRethinkDBStore_AddEmail(t *testing.T) {
	err := rethinkStore.RemoveEmail("foo")
	assert.NilError(t, err)

	// duplicate to test when definitely empty
	err = rethinkStore.RemoveEmail("foo")
	assert.NilError(t, err)

	email, ok, err := rethinkStore.GetEmail("foo")
	assert.Equal(t, email, "")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)

	err = rethinkStore.AddEmail("foo", "bar")
	assert.NilError(t, err)

	email, ok, err = rethinkStore.GetEmail("foo")
	assert.Equal(t, email, "bar")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = rethinkStore.AddEmail("foo", "bar,jitsu")
	assert.NilError(t, err)

	email, ok, err = rethinkStore.GetEmail("foo")
	assert.Equal(t, email, "bar,jitsu")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = rethinkStore.RemoveEmail("foo")
	assert.NilError(t, err)

	email, ok, err = rethinkStore.GetEmail("foo")
	assert.Equal(t, email, "")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestRethinkDBStore_ListEmail(t *testing.T) {
	rethinkStore.RemoveEmail("foo1")
	rethinkStore.RemoveEmail("foo2")
	rethinkStore.RemoveEmail("foo3")
	rethinkStore.RemoveEmail("foo4")
	rethinkStore.RemoveEmail("foo5")
	defer rethinkStore.RemoveEmail("foo1")
	defer rethinkStore.RemoveEmail("foo2")
	defer rethinkStore.RemoveEmail("foo3")
	defer rethinkStore.RemoveEmail("foo4")
	defer rethinkStore.RemoveEmail("foo5")

	list, err := rethinkStore.ListEmail()
	assert.Equal(t, len(list), 0)
	assert.NilError(t, err)

	rethinkStore.AddEmail("foo1", "bar1")
	rethinkStore.AddEmail("foo2", "bar2")
	rethinkStore.AddEmail("foo3", "bar3")
	rethinkStore.AddEmail("foo4", "bar4")
	rethinkStore.AddEmail("foo5", "bar5")

	expectedList := make([]sentry_store.CallsignEmail, 5, 5)
	expectedList[0] = sentry_store.CallsignEmail{"foo1", "bar1"}
	expectedList[1] = sentry_store.CallsignEmail{"foo2", "bar2"}
	expectedList[2] = sentry_store.CallsignEmail{"foo3", "bar3"}
	expectedList[3] = sentry_store.CallsignEmail{"foo4", "bar4"}
	expectedList[4] = sentry_store.CallsignEmail{"foo5", "bar5"}

	list, err = rethinkStore.ListEmail()
	assert.DeepEqual(t, list, expectedList)
	assert.NilError(t, err)

	rethinkStore.RemoveEmail("foo4")
	rethinkStore.RemoveEmail("foo5")

	expectedList = expectedList[0:3]

	list, err = rethinkStore.ListEmail()
	assert.DeepEqual(t, list, expectedList)
	assert.NilError(t, err)

	rethinkStore.RemoveEmail("foo1")
	rethinkStore.RemoveEmail("foo2")
	rethinkStore.RemoveEmail("foo3")

	list, err = rethinkStore.ListEmail()
	assert.Equal(t, len(list), 0)
	assert.NilError(t, err)
}
