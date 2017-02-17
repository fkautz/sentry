package sentry_goleveldb

import (
	"fmt"
	"github.com/fkautz/sentry/sentrylib/sentry_store"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/util"
	"strings"
	"time"
)

type goLevelDB struct {
	db *leveldb.DB
}

var NotImplementedError error = errors.New("Not Implemented")

func NewGoLevelDB(file string) (sentry_store.Store, error) {
	db, err := leveldb.OpenFile(file, nil)
	if err != nil {
		return nil, err
	}
	return &goLevelDB{
		db: db,
	}, nil
}

func (store *goLevelDB) AddLive(callsign string) error {
	return store.add("live", callsign, time.Now())
}

func (store *goLevelDB) AddDead(callsign string, ts time.Time) error {
	return store.add("dead", callsign, ts)
}

func (store *goLevelDB) add(prefix, callsign string, ts time.Time) error {
	ts = ts.UTC()
	key := fmt.Sprintf("%s-%s", prefix, callsign)
	value, err := ts.MarshalBinary()
	if err != nil {
		return err
	}
	return store.db.Put([]byte(key), value, nil)
}

func (store *goLevelDB) CountLive() (int, error) {
	return store.count("live")
}

func (store *goLevelDB) CountDead() (int, error) {
	return store.count("dead")
}

func (store *goLevelDB) count(prefix string) (int, error) {
	iter := store.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	count := 0
	for iter.Next() {
		count++
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (store *goLevelDB) GetLive(callsign string) (time.Time, bool, error) {
	return store.get("live", callsign)
}

func (store *goLevelDB) GetDead(callsign string) (time.Time, bool, error) {
	return store.get("dead", callsign)
}

func (store *goLevelDB) get(prefix, callsign string) (time.Time, bool, error) {
	key := fmt.Sprintf("%s-%s", prefix, callsign)
	val, err := store.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return time.Time{}, false, nil
	} else if err != nil {
		return time.Time{}, false, err
	}
	ts := time.Now()
	err = ts.UnmarshalBinary(val)
	if err != nil {
		return time.Time{}, false, err
	}
	return ts, true, nil
}

func (store *goLevelDB) ListLive(ts time.Time) ([]sentry_store.CallsignTime, error) {
	return store.list("live", ts)
}

func (store *goLevelDB) ListDead() ([]sentry_store.CallsignTime, error) {
	return store.list("dead", time.Now())
}

func (store *goLevelDB) list(prefix string, ts time.Time) ([]sentry_store.CallsignTime, error) {
	iter := store.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	result := make([]sentry_store.CallsignTime, 0, 1000)
	for iter.Next() {
		callsign := strings.TrimPrefix(string(iter.Key()), prefix+"-")
		lastSeen := time.Now()
		err := lastSeen.UnmarshalBinary(iter.Value())
		if err != nil {
			continue
		}
		if lastSeen.Before(ts) {
			result = append(result, sentry_store.CallsignTime{callsign, lastSeen})
		}
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (store *goLevelDB) RemoveLive(callsign string, ts time.Time) error {
	return store.remove("live", callsign, ts)
}

func (store *goLevelDB) RemoveDead(callsign string) error {
	return store.remove("dead", callsign, time.Now())
}

func (store *goLevelDB) remove(prefix, callsign string, ts time.Time) error {
	tx, err := store.db.OpenTransaction()
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s-%s", prefix, callsign)
	val, err := tx.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		tx.Discard()
		return nil
	}
	if err != nil {
		tx.Discard()
		return err
	}
	lastSeen := time.Now()
	lastSeen.UnmarshalBinary(val)
	if lastSeen.Before(ts) {
		err = tx.Delete([]byte(key), nil)
		if err != nil {
			tx.Discard()
			return nil
		}
		tx.Commit()
		return nil
	}
	tx.Discard()
	return nil
}

func (store *goLevelDB) AddEmail(callsign, email string) error {
	return store.db.Put([]byte("email-"+callsign), []byte(email), nil)
}
func (store *goLevelDB) GetEmail(callsign string) (string, bool, error) {
	val, err := store.db.Get([]byte("email-"+callsign), nil)
	if err == leveldb.ErrNotFound {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return string(val), true, nil
}
func (store *goLevelDB) ListEmail() ([]sentry_store.CallsignEmail, error) {
	iter := store.db.NewIterator(util.BytesPrefix([]byte("email-")), nil)
	result := make([]sentry_store.CallsignEmail, 0, 1000)
	for iter.Next() {
		callsign := strings.TrimPrefix(string(iter.Key()), "email-")
		email := string(iter.Value())
		result = append(result, sentry_store.CallsignEmail{callsign, email})
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return nil, err
	}
	return result, nil
}
func (store *goLevelDB) RemoveEmail(callsign string) error {
	key := []byte("email-" + callsign)
	return store.db.Delete(key, nil)
}
