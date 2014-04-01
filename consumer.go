package loggregator_consumer

import (
	"errors"
	"crypto/tls"
	"net/http"
	"net/url"
	"code.google.com/p/go.net/websocket"
	"github.com/cloudfoundry/loggregatorlib/logmessage"
)

type LoggregatorConection interface {
	Tail() (<- chan *logmessage.LogMessage, <-chan error)
	Close() error
}

type connection struct {
	endpoint string
	ws *websocket.Conn
}

func NewConnection(endpoint string, tlsConfig *tls.Config, proxy func(*http.Request) (*url.URL, error)) LoggregatorConection {
	return &connection{endpoint: endpoint}
}

func (conn *connection) Tail() (<-chan *logmessage.LogMessage, <-chan error) {
	incomingChan := make(chan *logmessage.LogMessage)
	errChan := make(chan error)

	wsConfig, err := websocket.NewConfig("ws://" + conn.endpoint, "http://localhost")
	if err == nil {
		conn.ws, err = websocket.DialConfig(wsConfig)
	}
	if err != nil {
		go func() {
			errChan <- err
			close(incomingChan)
			close(errChan)
		}()
		return incomingChan, errChan
	}


	go conn.listenForMessages(incomingChan, errChan)

	return incomingChan, errChan
}

func (conn *connection) Close() error {
	if conn.ws == nil {
		return errors.New("connection does not exist")
	}

	return conn.ws.Close()
}

func (conn *connection) listenForMessages(msgChan chan<- *logmessage.LogMessage, errChan chan<- error) {
	defer close(msgChan)
	defer close(errChan)

	for {
		var data []byte
		err := websocket.Message.Receive(conn.ws, &data)
		if err != nil {
			errChan <- err
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
