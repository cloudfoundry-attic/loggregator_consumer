package loggregator_consumer_test

import (
	consumer "github.com/cloudfoundry/loggregator_consumer"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http/httptest"
	"code.google.com/p/go.net/websocket"
	"github.com/cloudfoundry/loggregatorlib/logmessage"
	"code.google.com/p/gogoprotobuf/proto"
	"time"
//	"fmt"
	"crypto/tls"
)

type FakeHandler struct {
	Messages []*logmessage.LogMessage
	called bool
	closeConnection chan bool
	closedConnectionError error
	messageReceived chan bool
	lastURL string
}

func (fh *FakeHandler) handle(conn *websocket.Conn) {
	fh.called = true
	request := conn.Request()
	fh.lastURL = request.URL.String()

	if fh.messageReceived != nil {
		go func() {
			for {
				buffer := make([]byte, 1024)
				_, err := conn.Read(buffer)

				if err == nil {
					fh.messageReceived <- true
				} else {
					break
				}
			}
		}()
	}

	for _, protoMessage := range fh.Messages {
		if protoMessage == nil {
			conn.Write([]byte{})
		} else {
			message, err := proto.Marshal(protoMessage)
			Expect(err).ToNot(HaveOccurred())

			conn.Write(message)
		}
	}

	<-fh.closeConnection
	conn.Close()
}

func createMessage(message string) *logmessage.LogMessage{
	messageType := logmessage.LogMessage_OUT
	sourceName := "DEA"
	timestamp := time.Now().UnixNano()
	return &logmessage.LogMessage{
		Message:     []byte(message),
		AppId:       proto.String("my-app-guid"),
		MessageType: &messageType,
		SourceName:  &sourceName,
		Timestamp:   proto.Int64(timestamp),
	}
}

var _ = Describe("Loggregator Consumer", func() {
	var (
		connection consumer.LoggregatorConnection
		endpoint string
		testServer *httptest.Server
		fakeHandler FakeHandler
		tlsSettings *tls.Config
	)

	BeforeEach(func() {
		fakeHandler = FakeHandler{}
		fakeHandler.closeConnection = make(chan bool)
	})

	AfterEach(func() {
		testServer.Close()
	})

	Describe("Tail", func() {
		Context("when there is no TLS Config or proxy setting", func() {
			BeforeEach(func() {
				testServer = httptest.NewServer(websocket.Handler(fakeHandler.handle))
				endpoint = testServer.Listener.Addr().String()
			})

			Context("when the connection can be established", func() {
				JustBeforeEach(func() {
					connection = consumer.NewConnection(endpoint, nil, nil)
				})

				It("connects to the loggregator server", func() {
					connection.Tail("")
					Expect(fakeHandler.called).To(BeTrue())

					close(fakeHandler.closeConnection)
				})

				It("receives messages on the incoming channel", func(done Done) {
					fakeHandler.Messages = []*logmessage.LogMessage{createMessage("hello")}
					incomingChan, _ := connection.Tail("")
					message := <-incomingChan

					Expect(message.Message).To(Equal([]byte("hello")))

					close(fakeHandler.closeConnection)
					close(done)
				})

				It("closes the channel after the server closes the connection", func(done Done) {
					incomingChan, errChan := connection.Tail("")
					fakeHandler.closeConnection <- true

					Eventually(errChan).Should(BeClosed())
					Eventually(incomingChan).Should(BeClosed())

					close(done)
				})

				It("sends a keepalive to the server", func(done Done) {
					fakeHandler.messageReceived = make(chan bool)
				    consumer.KeepAlive = 10 * time.Millisecond
					connection.Tail("")

					Eventually(fakeHandler.messageReceived).Should(Receive())
					Eventually(fakeHandler.messageReceived).Should(Receive())

					close(fakeHandler.closeConnection)
					close(done)
				})

				It("sends messages for a specific app", func() {
					connection.Tail("app-guid")

					Expect(fakeHandler.lastURL).To(ContainSubstring("/tail/?app=app-guid"))
					close(fakeHandler.closeConnection)
				})

				Context("when the message fails to parse", func() {
					It("sends an error but continues to read messages", func(done Done) {
						fakeHandler.Messages = []*logmessage.LogMessage{nil, createMessage("hello")}
						incomingChan, errChan := connection.Tail("")

						err := <-errChan
						message := <-incomingChan

						Expect(err).ToNot(BeNil())
						Expect(message.Message).To(Equal([]byte("hello")))

						close(fakeHandler.closeConnection)
						close(done)
					})
				})
			})

			Context("when the connection cannot be established", func() {
				It("has an error if the websocket connection cannot be made", func(done Done) {
					endpoint = "!!!bad-endpoint"
					connection = consumer.NewConnection(endpoint, nil, nil)
					_, errChan := connection.Tail("")

					err := <-errChan

					Expect(err).ToNot(BeNil())

					close(fakeHandler.closeConnection)
					close(done)
				})
			})
		})

		Context("when SSL settings are passed in", func() {
			BeforeEach(func() {
				testServer = httptest.NewTLSServer(websocket.Handler(fakeHandler.handle))
				endpoint = testServer.Listener.Addr().String()
			})

			JustBeforeEach(func() {
				tlsSettings = &tls.Config{InsecureSkipVerify: true}
				connection = consumer.NewConnection(endpoint, tlsSettings, nil)
			})

			It("connects using those settings", func() {
				_, errChan := connection.Tail("")
				close(fakeHandler.closeConnection)

				_, ok := <-errChan
				Expect(ok).To(BeFalse())
			})
		})
	})

	Describe("Close", func() {
		BeforeEach(func() {
			testServer = httptest.NewServer(websocket.Handler(fakeHandler.handle))
			endpoint = testServer.Listener.Addr().String()
		})

	    Context("when a connection is not open", func() {
	        It("returns an error", func() {
				connection = consumer.NewConnection(endpoint, nil, nil)
				err := connection.Close()

				Expect(err.Error()).To(Equal("connection does not exist"))
	        })
	    })

		Context("when a connection is open", func() {
		    It("closes any open channels", func(done Done) {
				fakeHandler.closeConnection = make(chan bool)
				connection = consumer.NewConnection(endpoint, nil, nil)
				incomingChan, errChan := connection.Tail("")
				connection.Close()

				Eventually(errChan).Should(BeClosed())
				Eventually(incomingChan).Should(BeClosed())

				close(fakeHandler.closeConnection)
				close(done)
			})
		})
	})
})
