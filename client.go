package sse

import (
	"net/http"
	"time"
)

// Client represents a web browser connection.
type Client struct {
	id,
	channel,
	lastEventID string

	request  *http.Request
	response http.ResponseWriter
	flusher  http.Flusher

	eventChan chan Event
	doneChan  chan bool
}

func newClient(channelName, clientId string, r *http.Request, w http.ResponseWriter) *Client {
	return &Client{
		clientId,
		channelName,
		r.Header.Get("Last-Event-ID"),
		r,
		w,
		w.(http.Flusher),
		make(chan Event),
		make(chan bool),
	}
}

// Id returns the ID of client
func (c *Client) Id() string {
	return c.id
}

// Channel returns the channel where this client is subscribed to.
func (c *Client) Channel() string {
	return c.channel
}

// LastEventID returns the ID of the last event sent.
func (c *Client) LastEventID() string {
	return c.lastEventID
}

// SendMessage sends a message to client.
// Deprecated
func (c *Client) SendMessage(message *Message) {
	c.lastEventID = message.id
	c.Publish(message)
}

func (c *Client) Publish(event Event) {
	c.eventChan <- event
}

func (c *Client) server(interval time.Duration, onClose func()) {
	heartbeat := time.NewTicker(interval)
stop:
	for {
		select {
		case <-c.request.Context().Done():
			break stop
		case <-heartbeat.C:
			go c.Publish(HeartbeatEvent())
		case event, open := <-c.eventChan:
			if !open {
				break stop
			}
			buf := EventBuffer(event)
			_, err := c.response.Write(buf.Bytes())
			if err != nil {
				//logrus.Errorf("unable to write to client %v: %v", c.id, err.Error())
				break stop
			}
			c.flusher.Flush()
		}
	}

	heartbeat.Stop()
	c.doneChan <- true
	onClose()
}

func (c *Client) Done() <-chan bool {
	return c.doneChan
}

func (c *Client) Close() {
	close(c.eventChan)
	close(c.doneChan)
}
