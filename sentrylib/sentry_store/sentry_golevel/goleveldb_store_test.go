package sentry_goleveldb

import (
	"github.com/docker/docker/pkg/testutil/assert"
	"github.com/fkautz/sentry/sentrylib/sentry_store"
	"testing"
	"time"
)

var leveldbTestStore sentry_store.Store

func init() {
	leveldbTestStore, _ = NewGoLevelDB("/tmp/test_lvldb.db")
}

func TestGoLevelDBStore_AddLiveNew(t *testing.T) {
	leveldbTestStore.RemoveLive("FOO", time.Now())
	defer leveldbTestStore.RemoveLive("FOO", time.Now().Add(1*time.Hour))
	ts1 := time.Now()
	err := leveldbTestStore.AddLive("FOO")
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := leveldbTestStore.GetLive("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestGoLevelDBStore_AddLiveExisting(t *testing.T) {
	defer leveldbTestStore.RemoveLive("FOO", time.Now().Add(1*time.Hour))
	err := leveldbTestStore.AddLive("FOO")
	ts1 := time.Now()
	err = leveldbTestStore.AddLive("FOO")
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := leveldbTestStore.GetLive("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestGoLevelDBStore_GetLiveNoKey(t *testing.T) {
	leveldbTestStore.RemoveLive("NOEXIST", time.Now())
	_, ok, err := leveldbTestStore.GetLive("NOEXIST")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestGoLevelDBStore_RemoveLive(t *testing.T) {
	err := leveldbTestStore.RemoveLive("FOO", time.Now())
	assert.NilError(t, err)

	err = leveldbTestStore.AddLive("FOO")
	assert.NilError(t, err)
	_, ok, err := leveldbTestStore.GetLive("FOO")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = leveldbTestStore.RemoveLive("FOO", time.Now())
	assert.NilError(t, err)

	_, ok, err = leveldbTestStore.GetLive("FOO")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestGoLevelDBStore_ListLive(t *testing.T) {
	leveldbTestStore.RemoveLive("FOO1", time.Now())
	leveldbTestStore.RemoveLive("FOO2", time.Now())
	leveldbTestStore.RemoveLive("FOO3", time.Now())
	leveldbTestStore.RemoveLive("FOO4", time.Now())
	leveldbTestStore.RemoveLive("FOO5", time.Now())
	defer leveldbTestStore.RemoveLive("FOO1", time.Now().Add(1*time.Hour))
	defer leveldbTestStore.RemoveLive("FOO2", time.Now().Add(1*time.Hour))
	defer leveldbTestStore.RemoveLive("FOO3", time.Now().Add(1*time.Hour))
	defer leveldbTestStore.RemoveLive("FOO4", time.Now().Add(1*time.Hour))
	defer leveldbTestStore.RemoveLive("FOO5", time.Now().Add(1*time.Hour))

	startTs := time.Now()

	list, err := leveldbTestStore.ListLive(time.Now())
	assert.NilError(t, err)
	assert.DeepEqual(t, list, make([]sentry_store.CallsignTime, 0))

	leveldbTestStore.AddLive("FOO1")
	leveldbTestStore.AddLive("FOO2")
	leveldbTestStore.AddLive("FOO3")
	ts := time.Now()

	list, err = leveldbTestStore.ListLive(ts)
	assert.NilError(t, err)
	assert.Equal(t, len(list), 3)
	assert.Equal(t, list[0].Callsign, "FOO1")
	assert.Equal(t, list[1].Callsign, "FOO2")
	assert.Equal(t, list[2].Callsign, "FOO3")

	leveldbTestStore.AddLive("FOO4")
	leveldbTestStore.AddLive("FOO5")

	list, err = leveldbTestStore.ListLive(ts)
	assert.Equal(t, len(list), 3)
	assert.Equal(t, list[0].Callsign, "FOO1")
	assert.Equal(t, list[1].Callsign, "FOO2")
	assert.Equal(t, list[2].Callsign, "FOO3")

	list, err = leveldbTestStore.ListLive(time.Now())
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

func TestGoLevelDBStore_CountLive(t *testing.T) {
	leveldbTestStore.RemoveLive("FOO1", time.Now())
	leveldbTestStore.RemoveLive("FOO2", time.Now())
	leveldbTestStore.RemoveLive("FOO3", time.Now())
	leveldbTestStore.RemoveLive("FOO4", time.Now())
	leveldbTestStore.RemoveLive("FOO5", time.Now())
	defer leveldbTestStore.RemoveLive("FOO1", time.Now().Add(1*time.Hour))
	defer leveldbTestStore.RemoveLive("FOO2", time.Now().Add(1*time.Hour))
	defer leveldbTestStore.RemoveLive("FOO3", time.Now().Add(1*time.Hour))
	defer leveldbTestStore.RemoveLive("FOO4", time.Now().Add(1*time.Hour))
	defer leveldbTestStore.RemoveLive("FOO5", time.Now().Add(1*time.Hour))

	count, err := leveldbTestStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)

	err = leveldbTestStore.AddLive("FOO1")
	assert.NilError(t, err)
	count, err = leveldbTestStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = leveldbTestStore.AddLive("FOO2")
	assert.NilError(t, err)
	count, err = leveldbTestStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = leveldbTestStore.AddLive("FOO3")
	assert.NilError(t, err)
	count, err = leveldbTestStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 3)

	err = leveldbTestStore.RemoveLive("FOO1", time.Now())
	assert.NilError(t, err)
	count, err = leveldbTestStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = leveldbTestStore.RemoveLive("FOO2", time.Now())
	assert.NilError(t, err)
	count, err = leveldbTestStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = leveldbTestStore.RemoveLive("FOO3", time.Now())
	assert.NilError(t, err)
	count, err = leveldbTestStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)
}

func TestGoLevelDBStore_AddDeadNew(t *testing.T) {
	leveldbTestStore.RemoveDead("FOO")
	defer leveldbTestStore.RemoveDead("FOO")
	ts1 := time.Now()
	err := leveldbTestStore.AddDead("FOO", time.Now())
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := leveldbTestStore.GetDead("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestGoLevelDBStore_AddDeadExisting(t *testing.T) {
	defer leveldbTestStore.RemoveDead("FOO")
	err := leveldbTestStore.AddDead("FOO", time.Now())
	ts1 := time.Now()
	err = leveldbTestStore.AddDead("FOO", time.Now())
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := leveldbTestStore.GetDead("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestGoLevelDBStore_GetDeadNoKey(t *testing.T) {
	leveldbTestStore.RemoveDead("NOEXIST")
	_, ok, err := leveldbTestStore.GetDead("NOEXIST")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestGoLevelDBStore_RemoveDead(t *testing.T) {
	err := leveldbTestStore.RemoveDead("FOO")
	assert.NilError(t, err)

	err = leveldbTestStore.AddDead("FOO", time.Now())
	assert.NilError(t, err)
	_, ok, err := leveldbTestStore.GetDead("FOO")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = leveldbTestStore.RemoveDead("FOO")
	assert.NilError(t, err)

	_, ok, err = leveldbTestStore.GetDead("FOO")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestGoLevelDBStore_ListDead(t *testing.T) {
	leveldbTestStore.RemoveDead("FOO1")
	leveldbTestStore.RemoveDead("FOO2")
	leveldbTestStore.RemoveDead("FOO3")
	leveldbTestStore.RemoveDead("FOO4")
	leveldbTestStore.RemoveDead("FOO5")
	defer leveldbTestStore.RemoveDead("FOO1")
	defer leveldbTestStore.RemoveDead("FOO2")
	defer leveldbTestStore.RemoveDead("FOO3")
	defer leveldbTestStore.RemoveDead("FOO4")
	defer leveldbTestStore.RemoveDead("FOO5")

	startTs := time.Now()

	list, err := leveldbTestStore.ListDead()
	assert.NilError(t, err)
	assert.DeepEqual(t, list, make([]sentry_store.CallsignTime, 0))

	leveldbTestStore.AddDead("FOO1", time.Now())
	leveldbTestStore.AddDead("FOO2", time.Now())
	leveldbTestStore.AddDead("FOO3", time.Now())

	list, err = leveldbTestStore.ListDead()
	assert.NilError(t, err)
	assert.Equal(t, len(list), 3)
	assert.Equal(t, list[0].Callsign, "FOO1")
	assert.Equal(t, list[1].Callsign, "FOO2")
	assert.Equal(t, list[2].Callsign, "FOO3")

	leveldbTestStore.AddDead("FOO4", time.Now())
	leveldbTestStore.AddDead("FOO5", time.Now())

	list, err = leveldbTestStore.ListDead()
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

func TestGoLevelDBStore_CountDead(t *testing.T) {
	leveldbTestStore.RemoveDead("FOO1")
	leveldbTestStore.RemoveDead("FOO2")
	leveldbTestStore.RemoveDead("FOO3")
	leveldbTestStore.RemoveDead("FOO4")
	leveldbTestStore.RemoveDead("FOO5")
	defer leveldbTestStore.RemoveDead("FOO1")
	defer leveldbTestStore.RemoveDead("FOO2")
	defer leveldbTestStore.RemoveDead("FOO3")
	defer leveldbTestStore.RemoveDead("FOO4")
	defer leveldbTestStore.RemoveDead("FOO5")

	count, err := leveldbTestStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)

	err = leveldbTestStore.AddDead("FOO1", time.Now())
	assert.NilError(t, err)
	count, err = leveldbTestStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = leveldbTestStore.AddDead("FOO2", time.Now())
	assert.NilError(t, err)
	count, err = leveldbTestStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = leveldbTestStore.AddDead("FOO3", time.Now())
	assert.NilError(t, err)
	count, err = leveldbTestStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 3)

	err = leveldbTestStore.RemoveDead("FOO1")
	assert.NilError(t, err)
	count, err = leveldbTestStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = leveldbTestStore.RemoveDead("FOO2")
	assert.NilError(t, err)
	count, err = leveldbTestStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = leveldbTestStore.RemoveDead("FOO3")
	assert.NilError(t, err)
	count, err = leveldbTestStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)
}

func TestGoLevelDBStore_AddEmail(t *testing.T) {
	err := leveldbTestStore.RemoveEmail("foo")
	assert.NilError(t, err)

	// duplicate to test when definitely empty
	err = leveldbTestStore.RemoveEmail("foo")
	assert.NilError(t, err)

	email, ok, err := leveldbTestStore.GetEmail("foo")
	assert.Equal(t, email, "")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)

	err = leveldbTestStore.AddEmail("foo", "bar")
	assert.NilError(t, err)

	email, ok, err = leveldbTestStore.GetEmail("foo")
	assert.Equal(t, email, "bar")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = leveldbTestStore.AddEmail("foo", "bar,jitsu")
	assert.NilError(t, err)

	email, ok, err = leveldbTestStore.GetEmail("foo")
	assert.Equal(t, email, "bar,jitsu")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = leveldbTestStore.RemoveEmail("foo")
	assert.NilError(t, err)

	email, ok, err = leveldbTestStore.GetEmail("foo")
	assert.Equal(t, email, "")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestGoLevelDBStore_ListEmail(t *testing.T) {
	leveldbTestStore.RemoveEmail("foo1")
	leveldbTestStore.RemoveEmail("foo2")
	leveldbTestStore.RemoveEmail("foo3")
	leveldbTestStore.RemoveEmail("foo4")
	leveldbTestStore.RemoveEmail("foo5")
	defer leveldbTestStore.RemoveEmail("foo1")
	defer leveldbTestStore.RemoveEmail("foo2")
	defer leveldbTestStore.RemoveEmail("foo3")
	defer leveldbTestStore.RemoveEmail("foo4")
	defer leveldbTestStore.RemoveEmail("foo5")

	list, err := leveldbTestStore.ListEmail()
	assert.Equal(t, len(list), 0)
	assert.NilError(t, err)

	leveldbTestStore.AddEmail("foo1", "bar1")
	leveldbTestStore.AddEmail("foo2", "bar2")
	leveldbTestStore.AddEmail("foo3", "bar3")
	leveldbTestStore.AddEmail("foo4", "bar4")
	leveldbTestStore.AddEmail("foo5", "bar5")

	expectedList := make([]sentry_store.CallsignEmail, 5, 5)
	expectedList[0] = sentry_store.CallsignEmail{"foo1", "bar1"}
	expectedList[1] = sentry_store.CallsignEmail{"foo2", "bar2"}
	expectedList[2] = sentry_store.CallsignEmail{"foo3", "bar3"}
	expectedList[3] = sentry_store.CallsignEmail{"foo4", "bar4"}
	expectedList[4] = sentry_store.CallsignEmail{"foo5", "bar5"}

	list, err = leveldbTestStore.ListEmail()
	assert.DeepEqual(t, list, expectedList)
	assert.NilError(t, err)

	leveldbTestStore.RemoveEmail("foo4")
	leveldbTestStore.RemoveEmail("foo5")

	expectedList = expectedList[0:3]

	list, err = leveldbTestStore.ListEmail()
	assert.DeepEqual(t, list, expectedList)
	assert.NilError(t, err)

	leveldbTestStore.RemoveEmail("foo1")
	leveldbTestStore.RemoveEmail("foo2")
	leveldbTestStore.RemoveEmail("foo3")

	list, err = leveldbTestStore.ListEmail()
	assert.Equal(t, len(list), 0)
	assert.NilError(t, err)
}
