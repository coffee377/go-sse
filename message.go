package sse

import (
	"bytes"
	"fmt"
	"strings"
)

// Message represents a name source message.
type Message struct {
	id,
	data,
	event string
	retry int
}

// SimpleMessage creates a simple name source message.
func SimpleMessage(data string) *Message {
	return NewMessage("", data, "")
}

// NewMessage creates a name source message.
func NewMessage(id, data, event string) *Message {
	return &Message{
		id,
		data,
		event,
		0,
	}
}

func (m *Message) GetId() string {
	return m.id
}

func (m *Message) GetName() string {
	return m.event
}

func (m *Message) GetData() string {
	return m.data
}

func (m *Message) GetRetry() int {
	return m.retry
}

func (m *Message) Prepare() []byte {
	buffer := EventBuffer(m)
	return buffer.Bytes()
}

// Buffer formats the message.
func (m *Message) Buffer() *bytes.Buffer {
	var buffer bytes.Buffer

	if len(m.id) > 0 {
		buffer.WriteString(fmt.Sprintf("id: %s\n", m.id))
	}

	if m.retry > 0 {
		buffer.WriteString(fmt.Sprintf("retry: %d\n", m.retry))
	}

	if len(m.event) > 0 {
		buffer.WriteString(fmt.Sprintf("event: %s\n", m.event))
	}

	if len(m.data) > 0 {
		buffer.WriteString(fmt.Sprintf("data: %s\n", strings.Replace(m.data, "\n", "\ndata: ", -1)))
	}

	buffer.WriteString("\n")

	return &buffer
}

// String returns the formated message as a string.
func (m *Message) String() string {
	return m.Buffer().String()
}

// Bytes returns the formated message as a byte array.
func (m *Message) Bytes() []byte {
	return m.Buffer().Bytes()
}
