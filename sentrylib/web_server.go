package sentrylib

import (
	"encoding/json"
	"github.com/fkautz/sentry/sentrylib/sentry_store"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type webServer struct {
	store sentry_store.Store
}

func NewWebServer(store sentry_store.Store) {
	router := mux.NewRouter()
	ws := webServer{store: store}
	router.HandleFunc("/api/dead", ws.findDead).Methods("GET")
	router.HandleFunc("/api/live", ws.findLive).Methods("GET")
	router.HandleFunc("/api/node/{node}", ws.findNode).Methods("GET")
	go http.ListenAndServe(":8080", router)

	router = mux.NewRouter()
	router.HandleFunc("/email", ws.listEmail).Methods("GET")
	router.HandleFunc("/email/{node}", ws.getEmailForNode).Methods("GET")
	router.HandleFunc("/email/{node}", ws.addEmail).Methods("PUT")
	router.HandleFunc("/email/{node}", ws.removeEmail).Methods("DELETE")
	go http.ListenAndServe("127.0.0.1:8081", router)
}

func (s webServer) findLive(w http.ResponseWriter, r *http.Request) {
	list, err := s.store.ListLive(time.Now())
	if err != nil {
		w.WriteHeader(501)
		w.Write([]byte(err.Error()))
		return
	}
	res, err := json.MarshalIndent(list, "", "    ")
	if err != nil {
		w.WriteHeader(501)
		w.Write([]byte(err.Error()))
	}
	w.Write(res)
}

func (s webServer) findDead(w http.ResponseWriter, r *http.Request) {
	list, err := s.store.ListDead()
	if err != nil {
		w.WriteHeader(501)
		w.Write([]byte(err.Error()))
		return
	}

	res, err := json.MarshalIndent(list, "", "    ")
	if err != nil {
		w.WriteHeader(501)
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(res)
}

type CallsignTimeLive struct {
	sentry_store.CallsignTime
	SeenRecently bool
}

func (s webServer) findNode(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	seenRecently := true
	if callsign, ok := vars["node"]; ok {
		ts, ok, err := s.store.GetLive(callsign)
		if err != nil {
			w.WriteHeader(501)
			w.Write([]byte(err.Error()))
		}
		if !ok {
			ts, ok, err = s.store.GetDead(callsign)
			if err != nil {
				w.WriteHeader(501)
				w.Write([]byte(err.Error()))
				return
			}
			if ok {
				seenRecently = false
			}
		}
		if !ok {
			w.WriteHeader(404)
			w.Write([]byte("Could not find node with callsign '" + callsign + "'"))
			return
		}
		ct := sentry_store.CallsignTime{callsign, ts}
		ctl := CallsignTimeLive{ct, seenRecently}
		res, err := json.MarshalIndent(ctl, "", "    ")
		if err != nil {
			w.WriteHeader(501)
			w.Write([]byte(err.Error()))
		}
		w.Write(res)
	}
}

func (s webServer) addEmail(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	log.Println("ADD =====", vars, "=====")
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
	}
	s.store.AddEmail(vars["node"], string(body))
	log.Println("'" + string(body) + "'")
}

func (s webServer) listEmail(w http.ResponseWriter, r *http.Request) {
	emails, err := s.store.ListEmail()
	if err != nil {
		w.WriteHeader(501)
		w.Write([]byte(err.Error()))
		return
	}
	res, err := json.MarshalIndent(emails, "", "    ")
	if err != nil {
		w.WriteHeader(501)
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(res)
}

func (s webServer) getEmailForNode(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	email, ok, err := s.store.GetEmail(vars["node"])
	if err != nil {
		w.WriteHeader(501)
		w.Write([]byte(err.Error()))
	}
	if ok {
		w.Write([]byte(email))
	}
}

func (s webServer) removeEmail(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	s.store.RemoveEmail(vars["node"])
}
