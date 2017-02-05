package sentrylib

import (
	"errors"
	"github.com/boltdb/bolt"
	"log"
	"time"
)

type boltStore struct {
	db *bolt.DB
}

func NewBoltStore(f string) (Store, error) {
	db, err := bolt.Open(f, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Panicln("Unable to open or create database")
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("live"))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte("emails"))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte("dead"))
		if err != nil {
			return err
		}
		return nil
	})
	return &boltStore{
		db: db,
	}, nil
}

func (store *boltStore) AddLive(callsign string) error {
	return store.add("live", callsign, time.Now())
}

func (store *boltStore) AddDead(callsign string, ts time.Time) error {
	return store.add("dead", callsign, ts)
}

func (store *boltStore) add(bucket, callsign string, ts time.Time) error {
	ts = ts.UTC()
	return store.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		if err != nil {
			return err
		}
		tsBytes, err := ts.MarshalBinary()
		if err != nil {
			return err
		}
		return bucket.Put([]byte(callsign), tsBytes)
	})
}

func (store *boltStore) GetLive(callsign string) (time.Time, bool, error) {
	return store.get("live", callsign)
}

func (store *boltStore) GetDead(callsign string) (time.Time, bool, error) {
	return store.get("dead", callsign)
}

func (store *boltStore) get(bucket, callsign string) (time.Time, bool, error) {
	var byteResult []byte
	err := store.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucket))
		if bucket == nil {
			return errors.New("Could not open bucket")
		}
		byteResult = bucket.Get([]byte(callsign))
		return nil
	})
	if err != nil {
		return time.Time{}, false, err
	}
	if byteResult == nil {
		return time.Time{}, false, err
	}

	ts := time.Now()
	err = ts.UnmarshalBinary(byteResult)
	if err != nil {
		return time.Time{}, false, err
	}
	return ts, true, nil
}
func (store *boltStore) RemoveLive(callsign string, ts time.Time) error {
	return store.remove("live", callsign, ts)
}

func (store *boltStore) RemoveDead(callsign string) error {
	return store.remove("dead", callsign, time.Now())
}

func (store *boltStore) remove(bucket, callsign string, ts time.Time) error {
	return store.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		tsbytes := bucket.Get([]byte(callsign))
		if tsbytes == nil {
			return nil
		}
		var lastSeen time.Time
		err = lastSeen.UnmarshalBinary(tsbytes)
		if err != nil {
			log.Println("Unable to parse time, forcing callsign deletion")
			bucket.Delete([]byte(callsign))
			return err
		}
		if lastSeen.Before(ts) || lastSeen.Equal(ts) {
			bucket.Delete([]byte(callsign))
		}
		return nil
	})
}
func (store *boltStore) ListLive(ts time.Time) ([]CallsignTime, error) {
	return store.list("live", ts)
}

func (store *boltStore) ListDead() ([]CallsignTime, error) {
	return store.list("dead", time.Now())
}

func (store *boltStore) list(bucket string, ts time.Time) ([]CallsignTime, error) {
	results := make([]CallsignTime, 0, 10000)

	err := store.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucket))
		if bucket == nil {
			return errors.New("Could not open bucket")
		}
		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var lastSeen time.Time
			err := lastSeen.UnmarshalBinary(v)
			if err != nil {
				continue
			}
			lastSeen = lastSeen.UTC()
			if lastSeen.Before(ts) || lastSeen.Equal(ts) {
				cst := CallsignTime{string(k), lastSeen}
				results = append(results, cst)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (store *boltStore) CountLive() (int, error) {
	return store.count("live")
}

func (store *boltStore) CountDead() (int, error) {
	return store.count("dead")
}

func (store *boltStore) count(bucket string) (int, error) {
	result := 0
	err := store.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucket))
		if bucket == nil {
			return errors.New("Could not open bucket")
		}
		result = bucket.Stats().KeyN
		return nil
	})
	if err != nil {
		return 0, err
	}
	return result, nil
}

func (store *boltStore) AddEmail(callsign, email string) error {
	return store.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("emails"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte(callsign), []byte(email))
	})
}

func (store *boltStore) GetEmail(callsign string) (string, bool, error) {
	var byteResult []byte
	err := store.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("emails"))
		if bucket == nil {
			return errors.New("Could not open bucket")
		}
		byteResult = bucket.Get([]byte(callsign))
		return nil
	})
	if err != nil {
		return "", false, err
	}
	if byteResult == nil {
		return "", false, nil
	}

	return string(byteResult), true, nil
}

func (store *boltStore) RemoveEmail(callsign string) error {
	return store.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("emails"))
		if err != nil {
			return err
		}
		return bucket.Delete([]byte(callsign))
	})
}

func (store *boltStore) ListEmail() ([]CallsignEmail, error) {
	emails := make([]CallsignEmail, 0, 1000)
	err := store.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("emails"))
		if bucket == nil {
			return errors.New("Unable to open emails bucket")
		}
		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			email := CallsignEmail{string(k), string(v)}
			emails = append(emails, email)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return emails, nil
}
