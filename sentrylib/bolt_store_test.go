package sentrylib

import (
	"github.com/docker/docker/pkg/testutil/assert"
	"testing"
	"time"
)

var boltTestStore Store

func init() {
	boltTestStore, _ = NewBoltStore("/tmp/test.db")
}

func TestBoltStore_AddLiveNew(t *testing.T) {
	boltTestStore.RemoveLive("FOO", time.Now())
	defer boltTestStore.RemoveLive("FOO", time.Now().Add(1*time.Hour))
	ts1 := time.Now()
	err := boltTestStore.AddLive("FOO")
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := boltTestStore.GetLive("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestBoltStore_AddLiveExisting(t *testing.T) {
	defer boltTestStore.RemoveLive("FOO", time.Now().Add(1*time.Hour))
	err := boltTestStore.AddLive("FOO")
	ts1 := time.Now()
	err = boltTestStore.AddLive("FOO")
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := boltTestStore.GetLive("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestBoltStore_GetLiveNoKey(t *testing.T) {
	boltTestStore.RemoveLive("NOEXIST", time.Now())
	_, ok, err := boltTestStore.GetLive("NOEXIST")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestBoltStore_RemoveLive(t *testing.T) {
	err := boltTestStore.RemoveLive("FOO", time.Now())
	assert.NilError(t, err)

	err = boltTestStore.AddLive("FOO")
	assert.NilError(t, err)
	_, ok, err := boltTestStore.GetLive("FOO")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = boltTestStore.RemoveLive("FOO", time.Now())
	assert.NilError(t, err)

	_, ok, err = boltTestStore.GetLive("FOO")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestBoltStore_ListLive(t *testing.T) {
	boltTestStore.RemoveLive("FOO1", time.Now())
	boltTestStore.RemoveLive("FOO2", time.Now())
	boltTestStore.RemoveLive("FOO3", time.Now())
	boltTestStore.RemoveLive("FOO4", time.Now())
	boltTestStore.RemoveLive("FOO5", time.Now())
	defer boltTestStore.RemoveLive("FOO1", time.Now().Add(1*time.Hour))
	defer boltTestStore.RemoveLive("FOO2", time.Now().Add(1*time.Hour))
	defer boltTestStore.RemoveLive("FOO3", time.Now().Add(1*time.Hour))
	defer boltTestStore.RemoveLive("FOO4", time.Now().Add(1*time.Hour))
	defer boltTestStore.RemoveLive("FOO5", time.Now().Add(1*time.Hour))

	startTs := time.Now()

	list, err := boltTestStore.ListLive(time.Now())
	assert.NilError(t, err)
	assert.DeepEqual(t, list, make([]CallsignTime, 0))

	boltTestStore.AddLive("FOO1")
	boltTestStore.AddLive("FOO2")
	boltTestStore.AddLive("FOO3")
	ts := time.Now()

	list, err = boltTestStore.ListLive(ts)
	assert.NilError(t, err)
	assert.Equal(t, len(list), 3)
	assert.Equal(t, list[0].Callsign, "FOO1")
	assert.Equal(t, list[1].Callsign, "FOO2")
	assert.Equal(t, list[2].Callsign, "FOO3")

	boltTestStore.AddLive("FOO4")
	boltTestStore.AddLive("FOO5")

	list, err = boltTestStore.ListLive(ts)
	assert.Equal(t, len(list), 3)
	assert.Equal(t, list[0].Callsign, "FOO1")
	assert.Equal(t, list[1].Callsign, "FOO2")
	assert.Equal(t, list[2].Callsign, "FOO3")

	list, err = boltTestStore.ListLive(time.Now())
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
	boltTestStore.RemoveLive("FOO1", time.Now())
	boltTestStore.RemoveLive("FOO2", time.Now())
	boltTestStore.RemoveLive("FOO3", time.Now())
	boltTestStore.RemoveLive("FOO4", time.Now())
	boltTestStore.RemoveLive("FOO5", time.Now())
	defer boltTestStore.RemoveLive("FOO1", time.Now().Add(1*time.Hour))
	defer boltTestStore.RemoveLive("FOO2", time.Now().Add(1*time.Hour))
	defer boltTestStore.RemoveLive("FOO3", time.Now().Add(1*time.Hour))
	defer boltTestStore.RemoveLive("FOO4", time.Now().Add(1*time.Hour))
	defer boltTestStore.RemoveLive("FOO5", time.Now().Add(1*time.Hour))

	count, err := boltTestStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)

	err = boltTestStore.AddLive("FOO1")
	assert.NilError(t, err)
	count, err = boltTestStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = boltTestStore.AddLive("FOO2")
	assert.NilError(t, err)
	count, err = boltTestStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = boltTestStore.AddLive("FOO3")
	assert.NilError(t, err)
	count, err = boltTestStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 3)

	err = boltTestStore.RemoveLive("FOO1", time.Now())
	assert.NilError(t, err)
	count, err = boltTestStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = boltTestStore.RemoveLive("FOO2", time.Now())
	assert.NilError(t, err)
	count, err = boltTestStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = boltTestStore.RemoveLive("FOO3", time.Now())
	assert.NilError(t, err)
	count, err = boltTestStore.CountLive()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)
}

func TestBoltStore_AddDeadNew(t *testing.T) {
	boltTestStore.RemoveDead("FOO")
	defer boltTestStore.RemoveDead("FOO")
	ts1 := time.Now()
	err := boltTestStore.AddDead("FOO", time.Now())
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := boltTestStore.GetDead("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestBoltStore_AddDeadExisting(t *testing.T) {
	defer boltTestStore.RemoveDead("FOO")
	err := boltTestStore.AddDead("FOO", time.Now())
	ts1 := time.Now()
	err = boltTestStore.AddDead("FOO", time.Now())
	ts2 := time.Now()
	assert.NilError(t, err)
	ts, ok, err := boltTestStore.GetDead("FOO")
	assert.NilError(t, err)
	assert.Equal(t, ok, true)
	assert.Equal(t, ts1.Before(ts) && ts.Before(ts2), true)
}

func TestBoltStore_GetDeadNoKey(t *testing.T) {
	boltTestStore.RemoveDead("NOEXIST")
	_, ok, err := boltTestStore.GetDead("NOEXIST")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestBoltStore_RemoveDead(t *testing.T) {
	err := boltTestStore.RemoveDead("FOO")
	assert.NilError(t, err)

	err = boltTestStore.AddDead("FOO", time.Now())
	assert.NilError(t, err)
	_, ok, err := boltTestStore.GetDead("FOO")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = boltTestStore.RemoveDead("FOO")
	assert.NilError(t, err)

	_, ok, err = boltTestStore.GetDead("FOO")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestBoltStore_ListDead(t *testing.T) {
	boltTestStore.RemoveDead("FOO1")
	boltTestStore.RemoveDead("FOO2")
	boltTestStore.RemoveDead("FOO3")
	boltTestStore.RemoveDead("FOO4")
	boltTestStore.RemoveDead("FOO5")
	defer boltTestStore.RemoveDead("FOO1")
	defer boltTestStore.RemoveDead("FOO2")
	defer boltTestStore.RemoveDead("FOO3")
	defer boltTestStore.RemoveDead("FOO4")
	defer boltTestStore.RemoveDead("FOO5")

	startTs := time.Now()

	list, err := boltTestStore.ListDead()
	assert.NilError(t, err)
	assert.DeepEqual(t, list, make([]CallsignTime, 0))

	boltTestStore.AddDead("FOO1", time.Now())
	boltTestStore.AddDead("FOO2", time.Now())
	boltTestStore.AddDead("FOO3", time.Now())

	list, err = boltTestStore.ListDead()
	assert.NilError(t, err)
	assert.Equal(t, len(list), 3)
	assert.Equal(t, list[0].Callsign, "FOO1")
	assert.Equal(t, list[1].Callsign, "FOO2")
	assert.Equal(t, list[2].Callsign, "FOO3")

	boltTestStore.AddDead("FOO4", time.Now())
	boltTestStore.AddDead("FOO5", time.Now())

	list, err = boltTestStore.ListDead()
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
	boltTestStore.RemoveDead("FOO1")
	boltTestStore.RemoveDead("FOO2")
	boltTestStore.RemoveDead("FOO3")
	boltTestStore.RemoveDead("FOO4")
	boltTestStore.RemoveDead("FOO5")
	defer boltTestStore.RemoveDead("FOO1")
	defer boltTestStore.RemoveDead("FOO2")
	defer boltTestStore.RemoveDead("FOO3")
	defer boltTestStore.RemoveDead("FOO4")
	defer boltTestStore.RemoveDead("FOO5")

	count, err := boltTestStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)

	err = boltTestStore.AddDead("FOO1", time.Now())
	assert.NilError(t, err)
	count, err = boltTestStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = boltTestStore.AddDead("FOO2", time.Now())
	assert.NilError(t, err)
	count, err = boltTestStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = boltTestStore.AddDead("FOO3", time.Now())
	assert.NilError(t, err)
	count, err = boltTestStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 3)

	err = boltTestStore.RemoveDead("FOO1")
	assert.NilError(t, err)
	count, err = boltTestStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 2)

	err = boltTestStore.RemoveDead("FOO2")
	assert.NilError(t, err)
	count, err = boltTestStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 1)

	err = boltTestStore.RemoveDead("FOO3")
	assert.NilError(t, err)
	count, err = boltTestStore.CountDead()
	assert.NilError(t, err)
	assert.Equal(t, count, 0)
}

func TestBoltStore_AddEmail(t *testing.T) {
	err := boltTestStore.RemoveEmail("foo")
	assert.NilError(t, err)

	// duplicate to test when definitely empty
	err = boltTestStore.RemoveEmail("foo")
	assert.NilError(t, err)

	email, ok, err := boltTestStore.GetEmail("foo")
	assert.Equal(t, email, "")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)

	err = boltTestStore.AddEmail("foo", "bar")
	assert.NilError(t, err)

	email, ok, err = boltTestStore.GetEmail("foo")
	assert.Equal(t, email, "bar")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = boltTestStore.AddEmail("foo", "bar,jitsu")
	assert.NilError(t, err)

	email, ok, err = boltTestStore.GetEmail("foo")
	assert.Equal(t, email, "bar,jitsu")
	assert.Equal(t, ok, true)
	assert.NilError(t, err)

	err = boltTestStore.RemoveEmail("foo")
	assert.NilError(t, err)

	email, ok, err = boltTestStore.GetEmail("foo")
	assert.Equal(t, email, "")
	assert.Equal(t, ok, false)
	assert.NilError(t, err)
}

func TestBoltStore_ListEmail(t *testing.T) {
	boltTestStore.RemoveEmail("foo1")
	boltTestStore.RemoveEmail("foo2")
	boltTestStore.RemoveEmail("foo3")
	boltTestStore.RemoveEmail("foo4")
	boltTestStore.RemoveEmail("foo5")
	defer boltTestStore.RemoveEmail("foo1")
	defer boltTestStore.RemoveEmail("foo2")
	defer boltTestStore.RemoveEmail("foo3")
	defer boltTestStore.RemoveEmail("foo4")
	defer boltTestStore.RemoveEmail("foo5")

	list, err := boltTestStore.ListEmail()
	assert.Equal(t, len(list), 0)
	assert.NilError(t, err)

	boltTestStore.AddEmail("foo1", "bar1")
	boltTestStore.AddEmail("foo2", "bar2")
	boltTestStore.AddEmail("foo3", "bar3")
	boltTestStore.AddEmail("foo4", "bar4")
	boltTestStore.AddEmail("foo5", "bar5")

	expectedList := make([]CallsignEmail, 5, 5)
	expectedList[0] = CallsignEmail{"foo1", "bar1"}
	expectedList[1] = CallsignEmail{"foo2", "bar2"}
	expectedList[2] = CallsignEmail{"foo3", "bar3"}
	expectedList[3] = CallsignEmail{"foo4", "bar4"}
	expectedList[4] = CallsignEmail{"foo5", "bar5"}

	list, err = boltTestStore.ListEmail()
	assert.DeepEqual(t, list, expectedList)
	assert.NilError(t, err)

	boltTestStore.RemoveEmail("foo4")
	boltTestStore.RemoveEmail("foo5")

	expectedList = expectedList[0:3]

	list, err = boltTestStore.ListEmail()
	assert.DeepEqual(t, list, expectedList)
	assert.NilError(t, err)

	boltTestStore.RemoveEmail("foo1")
	boltTestStore.RemoveEmail("foo2")
	boltTestStore.RemoveEmail("foo3")

	list, err = boltTestStore.ListEmail()
	assert.Equal(t, len(list), 0)
	assert.NilError(t, err)
}
