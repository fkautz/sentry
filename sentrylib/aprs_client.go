package sentrylib

import (
	"bufio"
	"fmt"
	"github.com/dustin/go-aprs"
	"log"
	"net"
	"time"
)

type AprsClient interface {
	Dial() error
	Next() bool
	Error() error
	Frame() (aprs.Frame, error)
	Close() error
}

type aprsClient struct {
	conn     net.Conn
	reader   *bufio.Reader
	server   string
	callsign string
	passcode string
	filter   string
	err      error
	frame    aprs.Frame
}

func NewAprsClient(server, callsign, passcode, filter string) AprsClient {
	return &aprsClient{
		server:   server,
		callsign: callsign,
		passcode: passcode,
		filter:   filter,
	}
}

func (client *aprsClient) Dial() error {
	var conn net.Conn
	var reader *bufio.Reader
	var err error
	connString := fmt.Sprintf("user %s pass %s filter %s\n", client.callsign, client.passcode, client.filter)
	for i := uint(0); i < 10; i++ {
		time.Sleep(1 << i * time.Second)
		log.Println("Dialing " + client.server)
		conn, err = net.Dial("tcp", client.server)
		if err != nil {
			log.Println(err)
			continue
		}
		_, err = conn.Write([]byte(connString))
		if err != nil {
			log.Println(err)
			continue
		}
		reader = bufio.NewReader(conn)
		break
	}
	if conn == nil || reader == nil {
		return err
	}
	client.conn = conn
	client.reader = reader
	return nil
}

func (client *aprsClient) Close() error {
	return client.conn.Close()
}

func (client *aprsClient) Next() bool {
	line := ""
	isPrefix := true
	var err error
	for isPrefix == true && err == nil {
		var lineBytes []byte
		lineBytes, isPrefix, err = client.reader.ReadLine()
		if err == nil {
			line = line + string(lineBytes)
		}
	}
	client.err = err
	if err != nil {
		return false
	}

	client.frame = aprs.ParseFrame(line)
	return true
}

func (client *aprsClient) Frame() (aprs.Frame, error) {
	return client.frame, client.err
}
func (client *aprsClient) Error() error {
	return client.err
}
