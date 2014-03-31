package loggregator_consumer

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"code.google.com/p/go.net/websocket"
)

type LoggregatorConection interface {
	Tail()
}

type Connection struct {
	endpoint string
}

func NewConnection(endpoint string, tlsConfig *tls.Config, proxy func(*http.Request) (*url.URL, error)) LoggregatorConection {
	return &Connection{endpoint: endpoint}
}

func (conn Connection) Tail() {
	wsConfig, err := websocket.NewConfig("ws://" + conn.endpoint, "http://localhost")
	if err != nil {
		println(err.Error())
	}
	ws, err := websocket.DialConfig(wsConfig)
	if err != nil {
		println(err.Error())
	}
	println(ws)
}
