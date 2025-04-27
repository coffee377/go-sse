package sse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type Event interface {
	// GetId 获取事件的唯一标识符
	GetId() string
	// GetName 获取事件的名称
	GetName() string
	// GetData 获取事件相关的数据
	GetData() string
	// GetRetry 获取事件的重试次数（前端）
	GetRetry() int
}

func EventBuffer(e Event, name ...string) bytes.Buffer {
	var buffer bytes.Buffer

	if len(e.GetId()) > 0 {
		buffer.WriteString(fmt.Sprintf("id: %s\n", strings.Replace(e.GetId(), "\n", "", -1)))
	}

	if e.GetRetry() > 0 {
		buffer.WriteString(fmt.Sprintf("retry: %s\n", strings.Replace(strconv.Itoa(e.GetRetry()), "\n", "", -1)))
	}

	if len(name) > 0 {
		buffer.WriteString(fmt.Sprintf("event: %s\n", strings.Replace(name[0], "\n", "", -1)))
	} else if len(e.GetName()) > 0 {
		buffer.WriteString(fmt.Sprintf("event: %s\n", strings.Replace(e.GetName(), "\n", "", -1)))
	}

	if len(e.GetData()) > 0 {
		buffer.WriteString(fmt.Sprintf("data: %s\n", strings.Replace(e.GetData(), "\n", "\ndata: ", -1)))
	}

	buffer.WriteString("\n")

	return buffer
}

type baseEvent struct {
	id    string
	name  string
	data  string
	retry int
}

func (b baseEvent) GetId() string {
	return b.id
}

func (b baseEvent) GetName() string {
	return b.name
}

func (b baseEvent) GetData() string {
	return b.data
}

func (b baseEvent) GetRetry() int {
	return b.retry
}

type StringEvent struct {
	baseEvent
}

func HeartbeatEvent() Event {
	return &baseEvent{"", "ping", "pong", 0}
}

type eventOpts struct {
	id    string
	retry int
}

// OptFunc 是一个函数类型，用于设置可选参数
type OptFunc func(*eventOpts)

// WithID 是一个可选参数，用于设置事件的 ID
func WithID(id string) OptFunc {
	return func(opts *eventOpts) {
		opts.id = id
	}
}

// WithRetry 是一个可选参数，用于设置事件的重试次数
func WithRetry(retry int) OptFunc {
	return func(opts *eventOpts) {
		opts.retry = retry
	}
}

// NewEvent creates an name source message.
func NewEvent(name string, data interface{}, opts ...OptFunc) Event {
	defOpts := eventOpts{}
	// 应用可选参数
	for _, opt := range opts {
		opt(&defOpts)
	}
	return &StringEvent{
		baseEvent{defOpts.id, name, prepareData(name, data), defOpts.retry},
	}
}

// prepare 函数根据 data 的类型进行不同的处理
func prepareData(name string, data interface{}) string {
	var buffer bytes.Buffer

	// 判断 data 的类型
	switch reflect.TypeOf(data).Kind() {
	case reflect.Int, reflect.Float64, reflect.Bool, reflect.String:
		// 基础类型，直接转换为 string
		buffer.WriteString(fmt.Sprint(data))
	case reflect.Pointer:
		// 指针类型，获取其元素类型
		if reflect.TypeOf(data).Elem().Implements(reflect.TypeOf((*Event)(nil)).Elem()) {
			// 如果是指向 Event 类型的指针，设置其 name
			event := reflect.ValueOf(data).Elem().Interface().(Event)
			buffer = EventBuffer(event, name)
		} else {
			// 其他指针类型，使用 JSON 序列化
			jsonData, err := json.Marshal(data)
			if err != nil {
				//fmt.Println(&quot;Error marshaling JSON:&quot;, err)
				//return nil
			}
			buffer.Write(jsonData)
		}
	default:
		// 其他类型，使用 JSON 序列化
		jsonData, err := json.Marshal(data)
		if err != nil {
			//fmt.Println(&quot;Error marshaling JSON:&quot;, err)
			//return nil
		}
		buffer.Write(jsonData)
	}

	return buffer.String()
}
