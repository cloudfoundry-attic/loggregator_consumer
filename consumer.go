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
	"sort"
	"time"
)

var (
	KeepAlive = 25 * time.Second
)

/* LoggregatorConnection represents the actions that can be performed against a loggregator server.
 */
type LoggregatorConnection interface {

	//	Tail listens indefinitely for log messages. It returns two channels; the first is populated
	//	with log messages, while the second contains errors (e.g. from parsing messages). It returns
	//	immediately. Call Close() to terminate the connection when you are finished listening.
	//
	//	Messages are presented in the order received from the loggregator server. Chronological or
	//	other ordering is not guaranteed. It is the responsibility of the consumer of these channels
	//	to provide any desired sorting mechanism.
	Tail(appGuid string, authToken string) (<-chan *logmessage.LogMessage, <-chan error)

	//	Recent connects to loggregator via its 'dump' endpoint and returns a slice of recent messages.
	//	It does not guarantee any order of the messages; they are in the order returned by loggregator.
	//
	//	The SortRecent method is provided to sort the data returned by this method.
	Recent(appGuid string, authToken string) ([]*logmessage.LogMessage, error)

	// Close terminates the websocket connection to loggregator.
	Close() error
}

type connection struct {
	endpoint  string
	tlsConfig *tls.Config
	ws        *websocket.Conn
}

/* NewConnection creates a new connection to a loggregator endpoint.
 */
func NewConnection(endpoint string, tlsConfig *tls.Config, proxy func(*http.Request) (*url.URL, error)) LoggregatorConnection {
	return &connection{endpoint: endpoint, tlsConfig: tlsConfig}
}

/*
Tail listens indefinitely for log messages. It returns two channels; the first is populated
with log messages, while the second contains errors (e.g. from parsing messages). It returns immediately.
Call Close() to terminate the connection when you are finished listening.

Messages are presented in the order received from the loggregator server. Chronological or other ordering
is not guaranteed. It is the responsibility of the consumer of these channels to provide any desired sorting
mechanism.
*/
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

/*
Recent connects to loggregator via its 'dump' endpoint and returns a slice of recent messages. It does not
guarantee any order of the messages; they are in the order returned by loggregator.

The SortRecent method is provided to sort the data returned by this method.
*/
func (conn *connection) Recent(appGuid string, authToken string) ([]*logmessage.LogMessage, error) {
	var err error

	dumpPath := fmt.Sprintf("/dump/?app=%s", appGuid)
	conn.ws, err = conn.establishWebsocketConnection(dumpPath, authToken)

	if err != nil {
		return nil, err
	}

	messages := []*logmessage.LogMessage{}
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

/* Close terminates the websocket connection to loggregator.
 */
func (conn *connection) Close() error {
	if conn.ws == nil {
		return errors.New("connection does not exist")
	}

	return conn.ws.Close()
}

/*
SortRecent sorts a slice of LogMessages by timestamp. The sort is stable, so
messages with the same timestamp are sorted in the order that they are received.

The input slice is sorted; the return value is simply a pointer to the same slice.
*/
func SortRecent(messages []*logmessage.LogMessage) []*logmessage.LogMessage {
	sort.Stable(logMessageSlice(messages))
	return messages
}

type logMessageSlice []*logmessage.LogMessage

func (lms logMessageSlice) Len() int {
	return len(lms)
}

func (lms logMessageSlice) Less(i, j int) bool {
	return *(lms[i]).Timestamp < *(lms[j]).Timestamp
}

func (lms logMessageSlice) Swap(i, j int) {
	lms[i], lms[j] = lms[j], lms[i]
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
