package main

import (
	"encoding/json"
	"github.com/alexandrevicenzi/go-sse"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

type Todo struct {
	MID   string `json:"mid"`
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func main() {
	_ = ss()
}

func ss() error {
	// Create SSE server
	s := sse.NewServer(&sse.Options{
		RetryInterval: int(5 * time.Second.Milliseconds()),
		Headers: map[string]string{
			"Access-Control-Allow-Origin": "*",
		},
		//Logger:        log.New(os.Stdout, "", log.LstdFlags),
	})
	defer s.Shutdown()

	// Configure the route
	http.Handle("/", http.FileServer(http.Dir("./static")))
	http.Handle("/sse/", s)

	// Send messages every 5 seconds
	go func() {
		for t := range time.Tick(time.Second) {
			s.Publish("/sse/channel-1", sse.SimpleMessage(t.Format("2006/02/01/ 15:04:05")))
		}
	}()

	go func() {

		var (
			i       = 0
			mod     = 0
			message *sse.Message
		)
		// 创建一个每 5 秒触发一次的 Ticker
		ticker := time.NewTicker(5 * time.Second)
		// 用于控制循环退出的通道
		done := make(chan bool)

		// 启动一个 goroutine 模拟一段时间后停止任务
		go func() {
			//time.Sleep(1 * time.Minute)
			//done <- true
		}()

		for {
			select {
			case <-done:
				// 当 done 通道接收到消息时，退出循环
				return
			case <-ticker.C:
				i++
				mod = i % 3
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				// 生成 0 到 99 之间的随机数
				randomNum := r.Intn(20)
				switch mod {
				case 1:
					b1, _ := json.Marshal([]Todo{
						{"d05b89f2f754484fac944f5dd1dc9329", "请购审批", randomNum},
					})
					message = sse.NewMessage(strconv.Itoa(i), string(b1), "todo")
				case 2:
					b2, _ := json.Marshal([]Todo{
						{"3e1fab31114e43acb9af3676093bb455", "采购审批", randomNum},
					})
					message = sse.NewMessage(strconv.Itoa(i), string(b2), "todo")
				case 0:
					b3, _ := json.Marshal([]Todo{
						{"b43a4f984e5e44039fa49c95f8c76668", "付款审批", randomNum},
					})
					message = sse.NewMessage(strconv.Itoa(i), string(b3), "todo")
				}

				s.Publish("/sse/uid", message)
			}

		}
	}()

	go func() {
		for range time.Tick(5 * time.Second) {
			total := s.ClientCount()
			log.Println("Total: ", total)
			log.Println(s.Channels())
			for _, channelName := range s.Channels() {
				if ch, ok := s.GetChannel(channelName); ok {
					log.Println(channelName, ch.ClientCount())
				}
			}
			log.Println("")
		}
	}()

	log.Println("Listening at :3001")
	return http.ListenAndServe(":3001", nil)
}
