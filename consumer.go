package loggregator_consumer

import (
	"code.google.com/p/go.net/websocket"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/cloudfoundry/loggregatorlib/logmessage"
	"io"
	"net/http"
	"net/url"
	"time"
)

var (
	KeepAlive = 25 * time.Second
)

type LoggregatorConnection interface {
	Tail(appGuid string, authToken string) (<-chan *logmessage.LogMessage, <-chan error)
	Recent(appGuid string, authToken string) ([]*logmessage.LogMessage, error)
	Close() error
}

type connection struct {
	endpoint  string
	tlsConfig *tls.Config
	ws        *websocket.Conn
}

func NewConnection(endpoint string, tlsConfig *tls.Config, proxy func(*http.Request) (*url.URL, error)) LoggregatorConnection {
	return &connection{endpoint: endpoint, tlsConfig: tlsConfig}
}

func (conn *connection) Tail(appGuid string, authToken string) (<-chan *logmessage.LogMessage, <-chan error) {
	incomingChan := make(chan *logmessage.LogMessage)
	errChan := make(chan error)

	go func() {
		defer close(incomingChan)
		defer close(errChan)

		var err error

		tailPath := fmt.Sprintf("/tail/?app=%s", appGuid)
		conn.ws, err = conn.establishWebsocketConnection(tailPath, authToken)
		if err != nil {
			errChan <- err
		} else {
			go conn.sendKeepAlive()
			conn.listenForMessages(incomingChan, errChan)
		}
	}()

	return incomingChan, errChan
}

func (conn *connection) Recent(appGuid string, authToken string) ([]*logmessage.LogMessage, error) {
	var err error

	dumpPath := fmt.Sprintf("/dump/?app=%s", appGuid)
	conn.ws, err = conn.establishWebsocketConnection(dumpPath, authToken)

	if err != nil {
		return nil, err
	}

	messages := make([]*logmessage.LogMessage, 0)
	messageChan := make(chan *logmessage.LogMessage)
	errorChan := make(chan error)

	go func() {
		conn.listenForMessages(messageChan, errorChan)
		close(messageChan)
		close(errorChan)
	}()

	var firstError error

drainLoop:
	for {
		select {
		case err, ok := <-errorChan:
			if !ok {
				break drainLoop
			}

			if firstError == nil {
				firstError = err
			}

		case msg, ok := <-messageChan:
			if !ok {
				break drainLoop
			}

			messages = append(messages, msg)
		}
	}

	return messages, firstError
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

func (conn *connection) establishWebsocketConnection(path string, authToken string) (*websocket.Conn, error) {
	var protocol string
	if conn.tlsConfig == nil {
		protocol = "ws://"
	} else {
		protocol = "wss://"
	}

	wsConfig, err := websocket.NewConfig(protocol+conn.endpoint+path, "http://localhost")
	if err != nil {
		return nil, err
	}

	wsConfig.TlsConfig = conn.tlsConfig
	wsConfig.Header.Add("Authorization", authToken)
	return websocket.DialConfig(wsConfig)
}
