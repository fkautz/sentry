package sentry_pg

import (
	"database/sql"
	"time"

	"github.com/fkautz/sentry/sentrylib/sentry_store"
	_ "github.com/lib/pq"
)

type postgresDBStore struct {
	db *sql.DB
}

func NewPostgresDB(connString string) (sentry_store.Store, error) {
	db, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, err
	}

	return &postgresDBStore{
		db: db,
	}, nil
}

func (store *postgresDBStore) AddLive(callsign string) error {
	return store.add("live", callsign, time.Now())
}

func (store *postgresDBStore) AddDead(callsign string, ts time.Time) error {
	return store.add("dead", callsign, ts)
}

func (store *postgresDBStore) add(prefix, callsign string, ts time.Time) error {
	_, err := store.db.Exec("INSERT INTO "+prefix+" (callsign, ts) VALUES ($1, $2) ON CONFLICT (callsign) DO UPDATE SET callsign = $1, ts = $2", callsign, ts.UTC())
	return err
}

func (store *postgresDBStore) CountLive() (int, error) {
	return store.count("live")
}

func (store *postgresDBStore) CountDead() (int, error) {
	return store.count("dead")
}

func (store *postgresDBStore) count(prefix string) (int, error) {
	res := store.db.QueryRow("SELECT count(*) FROM " + prefix)
	count := 0
	err := res.Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (store *postgresDBStore) GetLive(callsign string) (time.Time, bool, error) {
	return store.get("live", callsign)
}

func (store *postgresDBStore) GetDead(callsign string) (time.Time, bool, error) {
	return store.get("dead", callsign)
}

func (store *postgresDBStore) get(prefix, callsign string) (time.Time, bool, error) {
	res := store.db.QueryRow("SELECT ts FROM "+prefix+" WHERE callsign=$1", callsign)
	ts := time.Time{}
	if err := res.Scan(&ts); err != nil {
		if err == sql.ErrNoRows {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, err
	}
	return ts, true, nil
}

func (store *postgresDBStore) ListLive(ts time.Time) ([]sentry_store.CallsignTime, error) {
	return store.list("live", ts)
}

func (store *postgresDBStore) ListDead() ([]sentry_store.CallsignTime, error) {
	return store.list("dead", time.Now())
}

func (store *postgresDBStore) list(prefix string, ts time.Time) ([]sentry_store.CallsignTime, error) {
	rows, err := store.db.Query("SELECT callsign, ts FROM "+prefix+" WHERE ts < $1", ts.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cts := make([]sentry_store.CallsignTime, 0)
	for rows.Next() {
		callsign := ""
		lastSeen := time.Time{}
		if err = rows.Scan(&callsign, &lastSeen); err != nil {
			return nil, err
		}
		ct := sentry_store.CallsignTime{callsign, lastSeen}
		cts = append(cts, ct)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cts, nil
}

func (store *postgresDBStore) RemoveLive(callsign string, ts time.Time) error {
	return store.remove("live", callsign, ts)
}

func (store *postgresDBStore) RemoveDead(callsign string) error {
	return store.remove("dead", callsign, time.Now())
}

func (store *postgresDBStore) remove(prefix, callsign string, ts time.Time) error {
	_, err := store.db.Exec("DELETE FROM "+prefix+" WHERE callsign=$1 AND ts <= $2", callsign, ts.UTC())
	return err
}

func (store *postgresDBStore) AddEmail(callsign, email string) error {
	_, err := store.db.Exec("INSERT INTO emails (callsign, email) VALUES ($1, $2) ON CONFLICT (callsign) DO UPDATE SET callsign=$1, email=$2", callsign, email)
	return err
}

func (store *postgresDBStore) GetEmail(callsign string) (string, bool, error) {
	res := store.db.QueryRow("SELECT email FROM emails WHERE callsign = $1", callsign)
	email := ""
	err := res.Scan(&email)
	if err == sql.ErrNoRows {
		return "", false, nil
	} else if err != nil {
		return "", false, err
	}
	return email, true, nil
}

func (store *postgresDBStore) ListEmail() ([]sentry_store.CallsignEmail, error) {
	rows, err := store.db.Query("SELECT callsign, email FROM emails")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ces := make([]sentry_store.CallsignEmail, 0)
	for rows.Next() {
		callsign := ""
		email := ""
		err := rows.Scan(&callsign, &email)
		if err != nil {
			return nil, err
		}
		ces = append(ces, sentry_store.CallsignEmail{callsign, email})
	}
	if rows.Err() != nil {
		return nil, err
	}
	return ces, nil
}

func (store *postgresDBStore) RemoveEmail(callsign string) error {
	_, err := store.db.Exec("DELETE FROM emails WHERE callsign = $1", callsign)
	return err
}
