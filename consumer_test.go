package loggregator_consumer_test

import (
	. "github.com/cloudfoundry/loggregator_consumer"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http/httptest"
	"code.google.com/p/go.net/websocket"
)

type FakeHandler struct {
	called bool
}

func (fh *FakeHandler) handle(conn *websocket.Conn) {
	fh.called = true
}

var _ = Describe("Loggregator Consumer", func() {
	Describe("Tail", func() {
		Context("when there is no TLS Config or proxy setting", func() {
			var (
				connection LoggregatorConection
				endpoint string
				testServer *httptest.Server
				fakeHandler FakeHandler
			)

			BeforeEach(func() {
				testServer = httptest.NewServer(websocket.Handler(fakeHandler.handle))
				endpoint = testServer.Listener.Addr().String()
				fakeHandler = FakeHandler{}
				connection = NewConnection(endpoint, nil, nil)
			})

			AfterEach(func() {
				testServer.Close()
			})

		    It("connects to the loggregator server", func() {
				connection.Tail()
				Expect(fakeHandler.called).To(BeTrue())
		    })
		})
	})
})
