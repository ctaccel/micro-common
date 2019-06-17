// Package rabbitmq provides a RabbitMQ broker
package rabbitmq

import (
	"context"
	"ctaccel.com/micro/common/log"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"sync"
	"time"

	"github.com/micro/go-micro/broker"
	"github.com/streadway/amqp"
)

type condition func() bool

type RmqBroker struct {
	conn           *rabbitMQConn
	addrs          []string
	opts           broker.Options
	prefetchCount  int
	prefetchGlobal bool
	mtx            sync.Mutex
	wg             sync.WaitGroup
}

type pullRequest struct {
	mtx          sync.Mutex
	mayRun       bool
	opts         broker.SubscribeOptions
	topic        string
	ch           *rabbitMQChannel
	durableQueue bool
	queueArgs    map[string]interface{}
	r            *RmqBroker
	fn           func(msg amqp.Delivery)
	cond         condition
	headers      map[string]interface{}
}

type publication struct {
	d amqp.Delivery
	m *broker.Message
	t string
}

func (p *publication) Ack() error {
	return p.d.Ack(false)
}

func (p *publication) Topic() string {
	return p.t
}

func (p *publication) Message() *broker.Message {
	return p.m
}

func (p *pullRequest) Options() broker.SubscribeOptions {
	return p.opts
}

func (p *pullRequest) Topic() string {
	return p.topic
}

func (p *pullRequest) Unpull() error {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.mayRun = false
	if p.ch != nil {
		return p.ch.Close()
	}
	return nil
}

func (p *pullRequest) dopull() {
	rePullDelay := time.Millisecond * 10

	minReChannelDelay := 100 * time.Millisecond
	maxReChannelDelay := 30 * time.Second
	expFactor := time.Duration(2)
	reChannelDelay := minReChannelDelay

	// connected to channel
	for {
		p.mtx.Lock()
		mayRun := p.mayRun
		p.mtx.Unlock()
		if !mayRun {
			// we are unsubscribed, showdown routine
			return
		}

		select {
		//check shutdown case
		case <-p.r.conn.close:
			//yep, its shutdown case
			return
			//wait until we reconect to rabbit
		case <-p.r.conn.waitConnection:
		}

		// it may crash (panic) in case of Consume without connection, so recheck it
		p.r.mtx.Lock()
		if !p.r.conn.connected {
			p.r.mtx.Unlock()
			continue
		}

		ch, err := p.r.conn.PullChannel(
			p.opts.Queue,
			p.topic,
			p.headers,
			p.queueArgs,
			p.durableQueue,
		)

		p.r.mtx.Unlock()
		if err != nil {
			if reChannelDelay > maxReChannelDelay {
				reChannelDelay = maxReChannelDelay
			}
			time.Sleep(reChannelDelay)
			reChannelDelay *= expFactor
			continue
		}

		p.mtx.Lock()
		p.ch = ch
		p.mtx.Unlock()
		break
	}

	//loop until disconnect
	for {
		p.mtx.Lock()
		mayRun := p.mayRun
		p.mtx.Unlock()
		if !mayRun {
			// we are unsubscribed, showdown routine
			return
		}

		select {
		//check shutdown case
		case <-p.r.conn.close:
			//yep, its shutdown case
			return
			//wait until we reconect to rabbit
		case <-p.r.conn.waitConnection:
		}

		// receiver busy
		if canPull := p.cond(); !canPull {
			continue
		}

		// it may crash (panic) in case of Pull without connection, so recheck it
		p.r.mtx.Lock()
		if !p.r.conn.connected {
			p.r.mtx.Unlock()
			continue
		}
		p.r.mtx.Unlock()

		time.Sleep(rePullDelay)

		d, ok, err := p.ch.PullQueue(p.opts.Queue, false)
		if !ok {
			log.Debug("rabbitmq pull empty")
			if err != nil {
				log.Error("rabbitmq pull message error", zap.String("error", err.Error()))
			}
		} else {
			fmt.Println(d)
			p.r.wg.Add(1)
			go func(d amqp.Delivery) {
				p.fn(d)
				p.r.wg.Done()
			}(d)
		}
	}
}

func NewBroker(options broker.Options, opts ...broker.Option) *RmqBroker {
	for _, o := range opts {
		o(&options)
	}

	return &RmqBroker{
		addrs: options.Addrs,
		opts:  options,
	}
}

func (r *RmqBroker) RegisterPuller(topic string, handler broker.Handler, cond condition, opts ...broker.SubscribeOption) error {
	var ackSuccess bool

	if r.conn == nil {
		return errors.New("not connected")
	}

	opt := broker.SubscribeOptions{
		AutoAck: true,
	}

	for _, o := range opts {
		o(&opt)
	}

	// Make sure context is setup
	if opt.Context == nil {
		opt.Context = context.Background()
	}

	ctx := opt.Context
	if subscribeContext, ok := ctx.Value(subscribeContextKey{}).(context.Context); ok && subscribeContext != nil {
		ctx = subscribeContext
	}

	requeueOnError := false
	requeueOnError, _ = ctx.Value(requeueOnErrorKey{}).(bool)

	durableQueue := false
	durableQueue, _ = ctx.Value(durableQueueKey{}).(bool)

	var qArgs map[string]interface{}
	if qa, ok := ctx.Value(queueArgumentsKey{}).(map[string]interface{}); ok {
		qArgs = qa
	}

	var headers map[string]interface{}
	if h, ok := ctx.Value(headersKey{}).(map[string]interface{}); ok {
		headers = h
	}

	if bval, ok := ctx.Value(ackSuccessKey{}).(bool); ok && bval {
		opt.AutoAck = false
		ackSuccess = true
	}

	fn := func(msg amqp.Delivery) {
		header := make(map[string]string)
		for k, v := range msg.Headers {
			header[k], _ = v.(string)
		}
		m := &broker.Message{
			Header: header,
			Body:   msg.Body,
		}
		err := handler(&publication{d: msg, m: m, t: msg.RoutingKey})
		if err == nil && ackSuccess && !opt.AutoAck {
			msg.Ack(false)
		} else if err != nil && !opt.AutoAck {
			msg.Nack(false, requeueOnError)
		}
	}

	pr := &pullRequest{topic: topic, opts: opt, mayRun: true, r: r, cond: cond,
		durableQueue: durableQueue, fn: fn, headers: headers, queueArgs: qArgs}

	go pr.dopull()

	return nil
}

func (r *RmqBroker) Options() broker.Options {
	return r.opts
}

func (r *RmqBroker) String() string {
	return "rabbitmq"
}

func (r *RmqBroker) Address() string {
	if len(r.addrs) > 0 {
		return r.addrs[0]
	}
	return ""
}

func (r *RmqBroker) Init(opts ...broker.Option) error {
	for _, o := range opts {
		o(&r.opts)
	}
	r.addrs = r.opts.Addrs
	return nil
}

func (r *RmqBroker) Connect() error {
	if r.conn == nil {
		r.conn = newRabbitMQConn(r.getExchange(), r.opts.Addrs, r.getPrefetchCount(), r.getPrefetchGlobal())
	}

	conf := defaultAmqpConfig

	if auth, ok := r.opts.Context.Value(externalAuth{}).(ExternalAuthentication); ok {
		conf.SASL = []amqp.Authentication{&auth}
	}

	conf.TLSClientConfig = r.opts.TLSConfig

	return r.conn.Connect(r.opts.Secure, &conf)
}

func (r *RmqBroker) Disconnect() error {
	if r.conn == nil {
		return errors.New("connection is nil")
	}
	ret := r.conn.Close()
	r.wg.Wait() // wait all goroutines
	return ret
}

func (r *RmqBroker) getExchange() exchange {

	ex := DefaultExchange

	if e, ok := r.opts.Context.Value(exchangeKey{}).(string); ok {
		ex.name = e
	}

	if d, ok := r.opts.Context.Value(durableExchange{}).(bool); ok {
		ex.durable = d
	}

	return ex
}

func (r *RmqBroker) getPrefetchCount() int {
	if e, ok := r.opts.Context.Value(prefetchCountKey{}).(int); ok {
		return e
	}
	return DefaultPrefetchCount
}

func (r *RmqBroker) getPrefetchGlobal() bool {
	if e, ok := r.opts.Context.Value(prefetchGlobalKey{}).(bool); ok {
		return e
	}
	return DefaultPrefetchGlobal
}
