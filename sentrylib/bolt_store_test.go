package sentrylib

import (
	"github.com/docker/docker/pkg/testutil/assert"
	"testing"
	"time"
)

var store Store

func init() {
	store, _ = NewBoltStore("/tmp/test.db")
}

func TestBoltStore_AddLiveNew(t *testing.T) {
	store.RemoveLive("FOO", time.Now())
	defer store.RemoveLive("FOO", time.Now().Add(1*time.Hour))
	ts1 := time.Now()
	err := store.AddLive("FOO")
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := store.GetLive("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestBoltStore_AddLiveExisting(t *testing.T) {
	defer store.RemoveLive("FOO", time.Now().Add(1*time.Hour))
	err := store.AddLive("FOO")
	ts1 := time.Now()
	err = store.AddLive("FOO")
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := store.GetLive("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestBoltStore_GetLiveNoKey(t *testing.T) {
	store.RemoveLive("NOEXIST", time.Now())
	_, ok, err := store.GetLive("NOEXIST")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestBoltStore_RemoveLive(t *testing.T) {
	err := store.RemoveLive("FOO", time.Now())
	assert.NilError(t, err)

	err = store.AddLive("FOO")
	assert.NilError(t, err)
	_, ok, err := store.GetLive("FOO")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = store.RemoveLive("FOO", time.Now())
	assert.NilError(t, err)

	_, ok, err = store.GetLive("FOO")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestBoltStore_ListLive(t *testing.T) {
	store.RemoveLive("FOO1", time.Now())
	store.RemoveLive("FOO2", time.Now())
	store.RemoveLive("FOO3", time.Now())
	store.RemoveLive("FOO4", time.Now())
	store.RemoveLive("FOO5", time.Now())
	defer store.RemoveLive("FOO1", time.Now().Add(1*time.Hour))
	defer store.RemoveLive("FOO2", time.Now().Add(1*time.Hour))
	defer store.RemoveLive("FOO3", time.Now().Add(1*time.Hour))
	defer store.RemoveLive("FOO4", time.Now().Add(1*time.Hour))
	defer store.RemoveLive("FOO5", time.Now().Add(1*time.Hour))

	startTs := time.Now()

	list, err := store.ListLive(time.Now())
	assert.NilError(t, err)
	assert.DeepEqual(t, list, make([]CallsignTime, 0))

	store.AddLive("FOO1")
	store.AddLive("FOO2")
	store.AddLive("FOO3")
	ts := time.Now()

	list, err = store.ListLive(ts)
	assert.NilError(t, err)
	assert.Equal(t, len(list), 3)
	assert.Equal(t, list[0].Callsign, "FOO1")
	assert.Equal(t, list[1].Callsign, "FOO2")
	assert.Equal(t, list[2].Callsign, "FOO3")

	store.AddLive("FOO4")
	store.AddLive("FOO5")

	list, err = store.ListLive(ts)
	assert.Equal(t, len(list), 3)
	assert.Equal(t, list[0].Callsign, "FOO1")
	assert.Equal(t, list[1].Callsign, "FOO2")
	assert.Equal(t, list[2].Callsign, "FOO3")

	list, err = store.ListLive(time.Now())
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

func TestBoltStore_CountLive(t *testing.T) {
	store.RemoveLive("FOO1", time.Now())
	store.RemoveLive("FOO2", time.Now())
	store.RemoveLive("FOO3", time.Now())
	store.RemoveLive("FOO4", time.Now())
	store.RemoveLive("FOO5", time.Now())
	defer store.RemoveLive("FOO1", time.Now().Add(1*time.Hour))
	defer store.RemoveLive("FOO2", time.Now().Add(1*time.Hour))
	defer store.RemoveLive("FOO3", time.Now().Add(1*time.Hour))
	defer store.RemoveLive("FOO4", time.Now().Add(1*time.Hour))
	defer store.RemoveLive("FOO5", time.Now().Add(1*time.Hour))

	count, err := store.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)

	err = store.AddLive("FOO1")
	assert.NilError(t, err)
	count, err = store.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = store.AddLive("FOO2")
	assert.NilError(t, err)
	count, err = store.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = store.AddLive("FOO3")
	assert.NilError(t, err)
	count, err = store.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 3)

	err = store.RemoveLive("FOO1", time.Now())
	assert.NilError(t, err)
	count, err = store.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = store.RemoveLive("FOO2", time.Now())
	assert.NilError(t, err)
	count, err = store.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = store.RemoveLive("FOO3", time.Now())
	assert.NilError(t, err)
	count, err = store.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)
}

func TestBoltStore_AddDeadNew(t *testing.T) {
	store.RemoveDead("FOO")
	defer store.RemoveDead("FOO")
	ts1 := time.Now()
	err := store.AddDead("FOO", time.Now())
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := store.GetDead("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestBoltStore_AddDeadExisting(t *testing.T) {
	defer store.RemoveDead("FOO")
	err := store.AddDead("FOO", time.Now())
	ts1 := time.Now()
	err = store.AddDead("FOO", time.Now())
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := store.GetDead("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestBoltStore_GetDeadNoKey(t *testing.T) {
	store.RemoveDead("NOEXIST")
	_, ok, err := store.GetDead("NOEXIST")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestBoltStore_RemoveDead(t *testing.T) {
	err := store.RemoveDead("FOO")
	assert.NilError(t, err)

	err = store.AddDead("FOO", time.Now())
	assert.NilError(t, err)
	_, ok, err := store.GetDead("FOO")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = store.RemoveDead("FOO")
	assert.NilError(t, err)

	_, ok, err = store.GetDead("FOO")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestBoltStore_ListDead(t *testing.T) {
	store.RemoveDead("FOO1")
	store.RemoveDead("FOO2")
	store.RemoveDead("FOO3")
	store.RemoveDead("FOO4")
	store.RemoveDead("FOO5")
	defer store.RemoveDead("FOO1")
	defer store.RemoveDead("FOO2")
	defer store.RemoveDead("FOO3")
	defer store.RemoveDead("FOO4")
	defer store.RemoveDead("FOO5")

	startTs := time.Now()

	list, err := store.ListDead()
	assert.NilError(t, err)
	assert.DeepEqual(t, list, make([]CallsignTime, 0))

	store.AddDead("FOO1", time.Now())
	store.AddDead("FOO2", time.Now())
	store.AddDead("FOO3", time.Now())

	list, err = store.ListDead()
	assert.NilError(t, err)
	assert.Equal(t, len(list), 3)
	assert.Equal(t, list[0].Callsign, "FOO1")
	assert.Equal(t, list[1].Callsign, "FOO2")
	assert.Equal(t, list[2].Callsign, "FOO3")

	store.AddDead("FOO4", time.Now())
	store.AddDead("FOO5", time.Now())

	list, err = store.ListDead()
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

func TestBoltStore_CountDead(t *testing.T) {
	store.RemoveDead("FOO1")
	store.RemoveDead("FOO2")
	store.RemoveDead("FOO3")
	store.RemoveDead("FOO4")
	store.RemoveDead("FOO5")
	defer store.RemoveDead("FOO1")
	defer store.RemoveDead("FOO2")
	defer store.RemoveDead("FOO3")
	defer store.RemoveDead("FOO4")
	defer store.RemoveDead("FOO5")

	count, err := store.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)

	err = store.AddDead("FOO1", time.Now())
	assert.NilError(t, err)
	count, err = store.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = store.AddDead("FOO2", time.Now())
	assert.NilError(t, err)
	count, err = store.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = store.AddDead("FOO3", time.Now())
	assert.NilError(t, err)
	count, err = store.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 3)

	err = store.RemoveDead("FOO1")
	assert.NilError(t, err)
	count, err = store.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = store.RemoveDead("FOO2")
	assert.NilError(t, err)
	count, err = store.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = store.RemoveDead("FOO3")
	assert.NilError(t, err)
	count, err = store.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)
}

func TestBoltStore_AddEmail(t *testing.T) {
	err := store.RemoveEmail("foo")
	assert.NilError(t, err)

	// duplicate to test when definitely empty
	err = store.RemoveEmail("foo")
	assert.NilError(t, err)

	email, ok, err := store.GetEmail("foo")
	assert.Equal(t, email, "")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)

	err = store.AddEmail("foo", "bar")
	assert.NilError(t, err)

	email, ok, err = store.GetEmail("foo")
	assert.Equal(t, email, "bar")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = store.AddEmail("foo", "bar,jitsu")
	assert.NilError(t, err)

	email, ok, err = store.GetEmail("foo")
	assert.Equal(t, email, "bar,jitsu")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = store.RemoveEmail("foo")
	assert.NilError(t, err)

	email, ok, err = store.GetEmail("foo")
	assert.Equal(t, email, "")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestBoltStore_ListEmail(t *testing.T) {
	store.RemoveEmail("foo1")
	store.RemoveEmail("foo2")
	store.RemoveEmail("foo3")
	store.RemoveEmail("foo4")
	store.RemoveEmail("foo5")
	defer store.RemoveEmail("foo1")
	defer store.RemoveEmail("foo2")
	defer store.RemoveEmail("foo3")
	defer store.RemoveEmail("foo4")
	defer store.RemoveEmail("foo5")

	list, err := store.ListEmail()
	assert.Equal(t, len(list), 0)
	assert.NilError(t, err)

	store.AddEmail("foo1", "bar1")
	store.AddEmail("foo2", "bar2")
	store.AddEmail("foo3", "bar3")
	store.AddEmail("foo4", "bar4")
	store.AddEmail("foo5", "bar5")

	expectedList := make([]CallsignEmail, 5, 5)
	expectedList[0] = CallsignEmail{"foo1", "bar1"}
	expectedList[1] = CallsignEmail{"foo2", "bar2"}
	expectedList[2] = CallsignEmail{"foo3", "bar3"}
	expectedList[3] = CallsignEmail{"foo4", "bar4"}
	expectedList[4] = CallsignEmail{"foo5", "bar5"}

	list, err = store.ListEmail()
	assert.DeepEqual(t, list, expectedList)
	assert.NilError(t, err)

	store.RemoveEmail("foo4")
	store.RemoveEmail("foo5")

	expectedList = expectedList[0:3]

	list, err = store.ListEmail()
	assert.DeepEqual(t, list, expectedList)
	assert.NilError(t, err)

	store.RemoveEmail("foo1")
	store.RemoveEmail("foo2")
	store.RemoveEmail("foo3")

	list, err = store.ListEmail()
	assert.Equal(t, len(list), 0)
	assert.NilError(t, err)
}
