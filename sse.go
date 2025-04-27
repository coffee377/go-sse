package sse

import (
	"github.com/google/uuid"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

// Server represents a server sent events server.
type Server struct {
	mu           sync.RWMutex
	options      *Options
	channels     map[string]*Channel
	addClient    chan *Client
	removeClient chan *Client
	shutdown     chan bool
	closeChannel chan string
}

// NewServer creates a new SSE server.
func NewServer(options *Options) *Server {
	if options == nil {
		options = &Options{
			Logger: log.New(os.Stdout, "go-sse: ", log.LstdFlags),
		}
	}

	if options.Logger == nil {
		options.Logger = log.New(ioutil.Discard, "", log.LstdFlags)
	}

	s := &Server{
		sync.RWMutex{},
		options,
		make(map[string]*Channel),
		make(chan *Client),
		make(chan *Client),
		make(chan bool),
		make(chan string),
	}

	go s.dispatch()

	return s
}

func (s *Server) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	client, ok := s.clientConnect(request, response)
	if !ok {
		return
	}

	s.setHeaders(request.Method, response)

	s.addClient <- client

	go client.server(15*time.Second, func() {
		s.removeClient <- client
	})
	<-client.Done()
	log.Printf("connection with client %v closed", client.Id())
}

// SendMessage broadcast a message to all clients in a channel.
// If channelName is an empty string, it will broadcast the message to all channels.
// Deprecated: use Server.PublishEvent instead.
func (s *Server) SendMessage(channelName string, message *Message) {
	s.Publish(channelName, message)
}

func (s *Server) Publish(channelName string, event Event) {
	if len(channelName) == 0 {
		s.options.Logger.Print("broadcasting message to all channels.")

		s.mu.RLock()

		for _, ch := range s.channels {
			ch.Publish(event)
		}

		s.mu.RUnlock()
	} else if ch, ok := s.getChannel(channelName); ok {
		ch.Publish(event)
		s.options.Logger.Printf("message sent to channel '%s'.", channelName)
	} else {
		s.options.Logger.Printf("message not sent because channel '%s' has no clients.", channelName)
	}
}

// Restart closes all channels and clients and allow new connections.
func (s *Server) Restart() {
	s.options.Logger.Print("restarting server.")
	s.close()
}

// Shutdown performs a graceful server shutdown.
func (s *Server) Shutdown() {
	s.shutdown <- true
}

// ClientCount returns the number of clients connected to this server.
func (s *Server) ClientCount() int {
	i := 0

	s.mu.RLock()

	for _, channel := range s.channels {
		i += channel.ClientCount()
	}

	s.mu.RUnlock()

	return i
}

// HasChannel returns true if the channel associated with name exists.
func (s *Server) HasChannel(name string) bool {
	_, ok := s.getChannel(name)
	return ok
}

// GetChannel returns the channel associated with name or nil if not found.
func (s *Server) GetChannel(name string) (*Channel, bool) {
	return s.getChannel(name)
}

// Channels returns a list of all channels to the server.
func (s *Server) Channels() []string {
	var channels []string

	s.mu.RLock()

	for name := range s.channels {
		channels = append(channels, name)
	}

	s.mu.RUnlock()

	return channels
}

// CloseChannel closes a channel.
func (s *Server) CloseChannel(name string) {
	s.closeChannel <- name
}

func (s *Server) clientConnect(r *http.Request, w http.ResponseWriter) (*Client, bool) {
	_, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return nil, false
	}
	channelName := r.URL.Path
	clientId := uuid.New().String()
	if s.options.ChannelNameFunc != nil {
		channelName = s.options.ChannelNameFunc(r)
	}
	if s.options.ClientIdFunc != nil {
		clientId = s.options.ClientIdFunc(r)
	}
	client := newClient(channelName, clientId, r, w)
	return client, true
}

func (s *Server) setHeaders(httpMethod string, w http.ResponseWriter) {
	h := w.Header()

	if s.options.hasHeaders() {
		for k, v := range s.options.Headers {
			h.Set(k, v)
		}
	}

	if httpMethod == "GET" {
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		h.Set("Connection", "keep-alive")
		h.Set("Transfer-Encoding", "chunked")
		h.Set("X-Accel-Buffering", "no")

		w.WriteHeader(http.StatusOK)
	} else if httpMethod != "OPTIONS" {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) addChannel(name string) *Channel {
	ch := newChannel(name)

	s.mu.Lock()
	s.channels[ch.name] = ch
	s.mu.Unlock()

	s.options.Logger.Printf("channel '%s' created.", ch.name)

	return ch
}

func (s *Server) removeChannel(ch *Channel) {
	s.mu.Lock()
	delete(s.channels, ch.name)
	s.mu.Unlock()

	ch.Close()

	s.options.Logger.Printf("channel '%s' closed.", ch.name)
}

func (s *Server) getChannel(name string) (*Channel, bool) {
	s.mu.RLock()
	ch, ok := s.channels[name]
	s.mu.RUnlock()
	return ch, ok
}

func (s *Server) close() {
	for _, ch := range s.channels {
		s.removeChannel(ch)
	}
	close(s.addClient)
	close(s.removeClient)
	close(s.shutdown)
	close(s.closeChannel)
}

func (s *Server) dispatch() {
	s.options.Logger.Print("server started.")

	for {
		select {

		// New client connected.
		case c := <-s.addClient:
			ch, exists := s.getChannel(c.channel)

			if !exists {
				ch = s.addChannel(c.channel)
			}

			ch.addClient(c)
			s.options.Logger.Printf("new client connected to channel '%s'.", ch.name)

		// Client disconnected.
		case c := <-s.removeClient:
			if ch, exists := s.getChannel(c.channel); exists {
				ch.removeClient(c)
				s.options.Logger.Printf("client disconnected from channel '%s'.", ch.name)

				if ch.ClientCount() == 0 {
					s.options.Logger.Printf("channel '%s' has no clients.", ch.name)
					s.removeChannel(ch)
				}
			}

		// Close channel and all clients in it.
		case channel := <-s.closeChannel:
			if ch, exists := s.getChannel(channel); exists {
				s.removeChannel(ch)
			} else {
				s.options.Logger.Printf("requested to close nonexistent channel '%s'.", channel)
			}

		// Event Source shutdown.
		case <-s.shutdown:
			s.close()
			s.options.Logger.Print("server stopped.")
			return
		}
	}
}
