package loggregator_consumer

import (
	"errors"
	"crypto/tls"
	"net/http"
	"net/url"
	"code.google.com/p/go.net/websocket"
	"github.com/cloudfoundry/loggregatorlib/logmessage"
	"time"
	"io"
)

var (
	KeepAlive = 25 * time.Second
)

type LoggregatorConnection interface {
	Tail() (<- chan *logmessage.LogMessage, <-chan error)
	Close() error
}

type connection struct {
	endpoint string
	tlsConfig *tls.Config
	ws *websocket.Conn
}

func NewConnection(endpoint string, tlsConfig *tls.Config, proxy func(*http.Request) (*url.URL, error)) LoggregatorConnection {
	return &connection{endpoint: endpoint, tlsConfig: tlsConfig}
}

func (conn *connection) Tail() (<-chan *logmessage.LogMessage, <-chan error) {
	incomingChan := make(chan *logmessage.LogMessage)
	errChan := make(chan error)

	var protocol string
	if conn.tlsConfig == nil {
		protocol = "ws://"
	} else {
		protocol = "wss://"
	}

	wsConfig, err := websocket.NewConfig(protocol + conn.endpoint, "http://localhost")
	wsConfig.TlsConfig = conn.tlsConfig
	if err == nil {
		conn.ws, err = websocket.DialConfig(wsConfig)
	}

	go func() {
		defer close(incomingChan)
		defer close(errChan)

		if err != nil {
			errChan <- err
		} else {
			conn.listenForMessages(incomingChan, errChan)
		}
	}()

	return incomingChan, errChan
}

func (conn *connection) Close() error {
	if conn.ws == nil {
		return errors.New("connection does not exist")
	}

	return conn.ws.Close()
}

func (conn *connection) sendKeepAlive() {
	for {
		err := websocket.Message.Send(conn.ws, "I'm alive!")
		if err != nil {
			return
		}
		time.Sleep(KeepAlive)
	}
}

func (conn *connection) listenForMessages(msgChan chan<- *logmessage.LogMessage, errChan chan<- error) {
	defer conn.ws.Close()
	go conn.sendKeepAlive()

	for {
		var data []byte
		err := websocket.Message.Receive(conn.ws, &data)
		if err != nil {
			if err != io.EOF {
				errChan <- err
			}

			break
		}

		msg, msgErr := logmessage.ParseMessage(data)
		if msgErr != nil {
			errChan <- msgErr
			continue
		}
		msgChan <- msg.GetLogMessage()
	}
}
