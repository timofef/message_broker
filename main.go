package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	MessageQueueLen = 100
	RequestQueueLen = 100
)

type Message struct {
	body string
}

type Request struct {
	canceled bool
	response chan string
}

type Broker struct {
	messageQueues map[string]chan *Message
	requestQueues map[string]chan *Request
	ctx           context.Context
	cancelWorkers context.CancelFunc
}

func NewBroker(ctx context.Context, cancel context.CancelFunc) *Broker {
	return &Broker{
		messageQueues: make(map[string]chan *Message),
		requestQueues: make(map[string]chan *Request),
		ctx:           ctx,
		cancelWorkers: cancel,
	}
}

func (b *Broker) AddMessage(queueName string, body string) {
	if _, exists := b.messageQueues[queueName]; !exists {
		b.messageQueues[queueName] = make(chan *Message, MessageQueueLen)
		b.requestQueues[queueName] = make(chan *Request, RequestQueueLen)
		go b.worker(b.ctx, queueName, b.messageQueues[queueName], b.requestQueues[queueName])
	}
	b.messageQueues[queueName] <- &Message{body: body}
}

func (b *Broker) TakeMessage(cancel context.CancelFunc, queueName string, withTimeout bool) chan string {
	if _, exists := b.messageQueues[queueName]; !exists {
		b.messageQueues[queueName] = make(chan *Message, MessageQueueLen)
		b.requestQueues[queueName] = make(chan *Request, RequestQueueLen)
		go b.worker(b.ctx, queueName, b.messageQueues[queueName], b.requestQueues[queueName])
	}

	responseChan := make(chan string)

	if !withTimeout {
		lm := len(b.messageQueues[queueName])
		lr := len(b.requestQueues[queueName])
		if lr != 0 || lm == 0 {
			cancel()
			return nil
		}
	}

	b.requestQueues[queueName] <- &Request{canceled: false, response: responseChan}

	return responseChan
}

func (b *Broker) worker(ctx context.Context, queueName string,
	messageQueue chan *Message, requestQueue chan *Request) {
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-requestQueue:
			if !r.canceled {
				m := <-messageQueue
				r.response <- m.body
			}
			if len(messageQueue) == 0 && len(requestQueue) == 0 {
				delete(b.messageQueues, queueName)
				delete(b.requestQueues, queueName)
				break
			}
		}
	}
}

func Put(b *Broker, w http.ResponseWriter, r *http.Request) {
	queueName := strings.TrimPrefix(r.URL.Path, "/")
	v := r.URL.Query().Get("v")
	if len(v) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	b.AddMessage(queueName, v)

	w.WriteHeader(http.StatusOK)
}

func Get(b *Broker, w http.ResponseWriter, r *http.Request) {
	queueName := strings.TrimPrefix(r.URL.Path, "/")

	ctx := r.Context()
	var cancel context.CancelFunc
	withTimeout := false

	if r.URL.Query().Has("timeout") {
		timeout, err := strconv.Atoi(r.URL.Query().Get("timeout"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		withTimeout = true

		ctx, cancel = context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	messageChan := b.TakeMessage(cancel, queueName, withTimeout)
	select {
	case m := <-messageChan:
		_, err := w.Write([]byte(m))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	case <-ctx.Done():
		w.WriteHeader(http.StatusNotFound)
	}
}

func main() {
	port := "80" // default
	if len(os.Args) >= 2 {
		port = os.Args[1]
	}

	ctx, cancel := context.WithCancel(context.Background())
	broker := NewBroker(ctx, cancel)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			Put(broker, w, r)
		case http.MethodGet:
			Get(broker, w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	fmt.Println(port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Println(err)
	}
}
