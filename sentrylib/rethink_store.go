package sentrylib

import (
	r "gopkg.in/gorethink/gorethink.v3"
	"time"
)

type rethinkDBStore struct {
	session   *r.Session
	db        string
}

type rethinkEntry struct {
	Callsign string    `gorethink:"callsign"`
	LastSeen time.Time `gorethink:"lastseen"`
	Id       string    `gorethink:"id,omitempty"`
}

type rethinkEmail struct {
	Callsign string `gorethink:"callsign"`
	Email    string `gorethink:"email"`
	Id       string `gorethink:"id,omitempty"`
}

func NewRethinkDB(address, db string) (Store, error) {
	session, err := r.Connect(r.ConnectOpts{
		Address: address,
	})
	if err != nil {
		return nil, err
	}

	r.DBDrop(db).Exec(session)
	r.DBCreate(db).Exec(session)

	r.DB(db).TableDrop("live").Exec(session)
	r.DB(db).TableDrop("dead").Exec(session)
	r.DB(db).TableDrop("email").Exec(session)

	r.DB(db).TableCreate("live").Exec(session)
	r.DB(db).TableCreate("dead").Exec(session)
	r.DB(db).TableCreate("email").Exec(session)

	r.DB(db).Table("live").IndexCreate("callsign").Exec(session)
	r.DB(db).Table("live").IndexCreate("lastseen").Exec(session)
	r.DB(db).Table("live").IndexWait().Exec(session)

	r.DB(db).Table("dead").IndexCreate("callsign").Exec(session)
	r.DB(db).Table("dead").IndexCreate("lastseen").Exec(session)
	r.DB(db).Table("dead").IndexWait().Exec(session)

	r.DB(db).Table("email").IndexCreate("callsign").Exec(session)
	r.DB(db).Table("email").IndexWait().Exec(session)

	store := &rethinkDBStore{
		session: session,
		db:      db,

	}

	return store, nil
}

func (store *rethinkDBStore) AddLive(callsign string) error {
	return store.add("live", callsign, time.Now())
}

func (store *rethinkDBStore) AddDead(callsign string, ts time.Time) error {
	return store.add("dead", callsign, ts)
}

func (store *rethinkDBStore) add(prefix, callsign string, ts time.Time) error {
	m, _, err := store.getByIndex(prefix, callsign)
	m.Callsign = callsign
	m.LastSeen = ts
	err = r.DB(store.db).Table(prefix).Insert(m, r.InsertOpts{Conflict: "replace"}).Exec(store.session)
	return err
}

func (store *rethinkDBStore) CountLive() (int, error) {
	return store.count("live")
}

func (store *rethinkDBStore) CountDead() (int, error) {
	return store.count("dead")
}

func (store *rethinkDBStore) count(prefix string) (int, error) {
	res, err := r.DB(store.db).Table(prefix).Count().Run(store.session)
	if res != nil {
		defer res.Close()
	}
	if err != nil {
		return 0, err
	}
	var count int
	err = res.One(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (store *rethinkDBStore) GetLive(callsign string) (time.Time, bool, error) {
	return store.get("live", callsign)
}

func (store *rethinkDBStore) GetDead(callsign string) (time.Time, bool, error) {
	return store.get("dead", callsign)
}

func (store *rethinkDBStore) get(prefix, callsign string) (time.Time, bool, error) {
	m, ok, err := store.getByIndex(prefix, callsign)
	if err != nil {
		return time.Time{}, false, err
	}
	if ok == false {
		return time.Time{}, false, nil
	}
	return m.LastSeen, true, nil
}

func (store *rethinkDBStore) getByIndex(prefix, callsign string) (rethinkEntry, bool, error) {
	res, err := r.DB(store.db).Table(prefix).GetAllByIndex("callsign", callsign).Run(store.session)
	if res != nil {
		defer res.Close()
	}
	if err != nil {
		return rethinkEntry{}, false, err
	}
	if res.IsNil() {
		return rethinkEntry{}, false, nil
	}
	m := rethinkEntry{}
	err = res.One(&m)
	return m, true, err
}

func (store *rethinkDBStore) ListLive(ts time.Time) ([]CallsignTime, error) {
	return store.list("live", ts)
}

func (store *rethinkDBStore) ListDead() ([]CallsignTime, error) {
	return store.list("dead", time.Now())
}

func (store *rethinkDBStore) list(prefix string, ts time.Time) ([]CallsignTime, error) {
	res, err := r.DB(store.db).Table(prefix).Between(time.Time{}, ts, r.BetweenOpts{Index:"lastseen"}).OrderBy("callsign").Run(store.session)
	if res != nil {
		defer res.Close()
	}
	if err != nil {
		return nil, err
	}
	filteredRows := make([]CallsignTime, 0)
	if res.IsNil() {
		return filteredRows, nil
	}
	var entry rethinkEntry
	for res.Next(&entry) {
		if entry.LastSeen.Before(ts) {
			filteredRows = append(filteredRows, CallsignTime{entry.Callsign, entry.LastSeen})
		}
	}
	return filteredRows, nil
}

func (store *rethinkDBStore) RemoveLive(callsign string, ts time.Time) error {
	return store.remove("live", callsign, ts)
}

func (store *rethinkDBStore) RemoveDead(callsign string) error {
	return store.remove("dead", callsign, time.Now())
}

func (store *rethinkDBStore) remove(prefix, callsign string, ts time.Time) error {
	m, ok, err := store.getByIndex(prefix, callsign)
	if err != nil {
		return err
	}
	if ok {
		return r.DB(store.db).Table(prefix).Get(m.Id).Delete(r.DeleteOpts{}).Exec(store.session)
	}
	return nil
}

func (store *rethinkDBStore) AddEmail(callsign, email string) error {
	m, _, err := store.getEmailByIndex(callsign)
	m.Callsign = callsign
	m.Email = email
	err = r.DB(store.db).Table("email").Insert(m, r.InsertOpts{Conflict: "replace"}).Exec(store.session)
	return err
}

func (store *rethinkDBStore) getEmailByIndex(callsign string) (rethinkEmail, bool, error) {
	res, err := r.DB(store.db).Table("email").GetAllByIndex("callsign", callsign).Run(store.session)
	if res != nil {
		defer res.Close()
	}
	if err != nil {
		return rethinkEmail{}, false, err
	}
	if res.IsNil() {
		return rethinkEmail{}, false, nil
	}
	m := rethinkEmail{}
	err = res.One(&m)
	return m, true, err
}

func (store *rethinkDBStore) GetEmail(callsign string) (string, bool, error) {
	res, ok, err := store.getEmailByIndex(callsign)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}
	return res.Email, true, nil
}

func (store *rethinkDBStore) ListEmail() ([]CallsignEmail, error) {
	res, err := r.DB(store.db).Table("email").OrderBy("callsign").Run(store.session)
	if res != nil {
		defer res.Close()
	}
	if err != nil {
		return nil, err
	}
	filteredRows := make([]CallsignEmail, 0)
	if res.IsNil() {
		return filteredRows, nil
	}
	var entry rethinkEmail
	for res.Next(&entry) {
		filteredRows = append(filteredRows, CallsignEmail{entry.Callsign, entry.Email})
	}
	return filteredRows, nil
}

func (store *rethinkDBStore) RemoveEmail(callsign string) error {
	m, ok, err := store.getEmailByIndex(callsign)
	if err != nil {
		return err
	}
	if ok {
		return r.DB(store.db).Table("email").Get(m.Id).Delete(r.DeleteOpts{}).Exec(store.session)
	}
	return nil
}
