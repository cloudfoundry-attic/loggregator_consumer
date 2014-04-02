package loggregator_consumer_test

import (
	"code.google.com/p/go.net/websocket"
	"code.google.com/p/gogoprotobuf/proto"
	"crypto/tls"
	consumer "github.com/cloudfoundry/loggregator_consumer"
	"github.com/cloudfoundry/loggregatorlib/logmessage"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http/httptest"
	"time"
)

type FakeHandler struct {
	Messages              []*logmessage.LogMessage
	called                bool
	closeConnection       chan bool
	closedConnectionError error
	messageReceived       chan bool
	lastURL               string
	authHeader            string
}

func (fh *FakeHandler) handle(conn *websocket.Conn) {
	fh.called = true
	request := conn.Request()
	fh.lastURL = request.URL.String()
	fh.authHeader = request.Header.Get("Authorization")

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

func createMessage(message string, timestamp int64) *logmessage.LogMessage {
	messageType := logmessage.LogMessage_OUT
	sourceName := "DEA"

	if timestamp == 0 {
		timestamp = time.Now().UnixNano()
	}

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
		connection  consumer.LoggregatorConsumer
		endpoint    string
		testServer  *httptest.Server
		fakeHandler FakeHandler
		tlsSettings *tls.Config

		appGuid      string
		authToken    string
		incomingChan <-chan *logmessage.LogMessage
		errChan      <-chan error
	)

	BeforeEach(func() {
		fakeHandler = FakeHandler{}
		fakeHandler.closeConnection = make(chan bool)
	})

	AfterEach(func() {
		testServer.Close()
	})

	Describe("SetOnConnectCallback", func() {
		BeforeEach(func() {
			testServer = httptest.NewServer(websocket.Handler(fakeHandler.handle))
			endpoint = testServer.Listener.Addr().String()
		})

		It("sets a callback and calls it when connecting", func() {
			called := false
			cb := func() { called = true }

			connection = consumer.New(endpoint, tlsSettings, nil)
			connection.SetOnConnectCallback(cb)
			connection.Tail(appGuid, authToken)

			Eventually(func() bool { return called }).Should(BeTrue())
			close(fakeHandler.closeConnection)
		})

		It("does not call the callback if the connection fails", func() {
			endpoint = "!!!bad-endpoint"

			called := false
			cb := func() { called = true }

			connection = consumer.New(endpoint, tlsSettings, nil)
			connection.SetOnConnectCallback(cb)
			connection.Tail(appGuid, authToken)

			Consistently(func() bool { return called }).Should(BeFalse())
			close(fakeHandler.closeConnection)
		})
	})

	Describe("Tail", func() {
		perform := func() {
			connection = consumer.New(endpoint, tlsSettings, nil)
			incomingChan, errChan = connection.Tail(appGuid, authToken)
		}

		Context("when there is no TLS Config or proxy setting", func() {
			BeforeEach(func() {
				testServer = httptest.NewServer(websocket.Handler(fakeHandler.handle))
				endpoint = testServer.Listener.Addr().String()
			})

			Context("when the connection can be established", func() {
				It("connects to the loggregator server", func() {
					defer close(fakeHandler.closeConnection)
					perform()

					Eventually(func() bool { return fakeHandler.called }).Should(BeTrue())
				})

				It("receives messages on the incoming channel", func(done Done) {
					defer close(fakeHandler.closeConnection)
					fakeHandler.Messages = []*logmessage.LogMessage{createMessage("hello", 0)}
					perform()
					message := <-incomingChan

					Expect(message.Message).To(Equal([]byte("hello")))

					close(done)
				})

				It("closes the channel after the server closes the connection", func(done Done) {
					perform()
					fakeHandler.closeConnection <- true

					Eventually(errChan).Should(BeClosed())
					Eventually(incomingChan).Should(BeClosed())

					close(done)
				})

				It("sends a keepalive to the server", func(done Done) {
					defer close(fakeHandler.closeConnection)
					fakeHandler.messageReceived = make(chan bool)
					consumer.KeepAlive = 10 * time.Millisecond
					perform()

					Eventually(fakeHandler.messageReceived).Should(Receive())
					Eventually(fakeHandler.messageReceived).Should(Receive())

					close(done)
				})

				It("sends messages for a specific app", func() {
					defer close(fakeHandler.closeConnection)
					appGuid = "app-guid"
					perform()

					Eventually(func() string { return fakeHandler.lastURL }).Should(ContainSubstring("/tail/?app=app-guid"))
				})

				It("sends an Authorization header with an access token", func() {
					defer close(fakeHandler.closeConnection)
					authToken = "auth-token"
					perform()

					Eventually(func() string { return fakeHandler.authHeader }).Should(Equal("auth-token"))
				})

				Context("when the message fails to parse", func() {
					It("sends an error but continues to read messages", func(done Done) {
						defer close(fakeHandler.closeConnection)
						fakeHandler.Messages = []*logmessage.LogMessage{nil, createMessage("hello", 0)}
						perform()

						err := <-errChan
						message := <-incomingChan

						Expect(err).ToNot(BeNil())
						Expect(message.Message).To(Equal([]byte("hello")))

						close(done)
					})
				})
			})

			Context("when the connection cannot be established", func() {
				BeforeEach(func() {
					endpoint = "!!!bad-endpoint"
				})

				It("has an error if the websocket connection cannot be made", func(done Done) {
					defer close(fakeHandler.closeConnection)
					perform()
					err := <-errChan

					Expect(err).ToNot(BeNil())

					close(done)
				})
			})
		})

		Context("when SSL settings are passed in", func() {
			BeforeEach(func() {
				testServer = httptest.NewTLSServer(websocket.Handler(fakeHandler.handle))
				endpoint = testServer.Listener.Addr().String()

				tlsSettings = &tls.Config{InsecureSkipVerify: true}
			})

			It("connects using those settings", func() {
				perform()
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
				connection = consumer.New(endpoint, nil, nil)
				err := connection.Close()

				Expect(err.Error()).To(Equal("connection does not exist"))
			})
		})

		Context("when a connection is open", func() {
			It("closes any open channels", func(done Done) {
				connection = consumer.New(endpoint, nil, nil)
				incomingChan, errChan := connection.Tail("", "")
				close(fakeHandler.closeConnection)

				Eventually(func() bool { return fakeHandler.called }).Should(BeTrue())

				connection.Close()

				Eventually(errChan).Should(BeClosed())
				Eventually(incomingChan).Should(BeClosed())

				close(done)
			})
		})
	})

	Describe("Recent", func() {
		var (
			appGuid     string
			authToken   string
			logMessages []*logmessage.LogMessage
			recentError error
		)

		perform := func() {
			connection = consumer.New(endpoint, nil, nil)
			close(fakeHandler.closeConnection)
			logMessages, recentError = connection.Recent(appGuid, authToken)
		}

		BeforeEach(func() {
			testServer = httptest.NewServer(websocket.Handler(fakeHandler.handle))
			endpoint = testServer.Listener.Addr().String()
		})

		Context("when the connection cannot be established", func() {
			It("returns an error", func() {
				endpoint = "invalid-endpoint"
				perform()

				Expect(recentError).ToNot(BeNil())
			})

		})

		Context("when the connection can be established", func() {
			It("connects to the loggregator server", func() {
				perform()

				Expect(fakeHandler.called).To(BeTrue())
			})

			It("returns messages from the server", func() {
				fakeHandler.Messages = []*logmessage.LogMessage{
					createMessage("test-message-0", 0),
					createMessage("test-message-1", 0),
				}
				perform()

				Expect(logMessages).To(HaveLen(2))
				Expect(logMessages[0].Message).To(Equal([]byte("test-message-0")))
				Expect(logMessages[1].Message).To(Equal([]byte("test-message-1")))
			})

			It("calls the right path on the loggregator endpoint", func() {
				appGuid = "app-guid"
				perform()

				Expect(fakeHandler.lastURL).To(ContainSubstring("/dump/?app=app-guid"))
			})
		})
	})

	Describe("SortRecent", func() {
		var messages []*logmessage.LogMessage

		BeforeEach(func() {
			messages = []*logmessage.LogMessage{createMessage("hello", 2), createMessage("konnichiha", 1)}
		})

		It("sorts messages", func() {
			sortedMessages := consumer.SortRecent(messages)

			Expect(*sortedMessages[0].Timestamp).To(Equal(int64(1)))
			Expect(*sortedMessages[1].Timestamp).To(Equal(int64(2)))
		})

		It("sorts using a stable algorithm", func() {
			messages = append(messages, createMessage("guten tag", 1))

			sortedMessages := consumer.SortRecent(messages)

			Expect(sortedMessages[0].Message).To(Equal([]byte("konnichiha")))
			Expect(sortedMessages[1].Message).To(Equal([]byte("guten tag")))
			Expect(sortedMessages[2].Message).To(Equal([]byte("hello")))
		})
	})
})
