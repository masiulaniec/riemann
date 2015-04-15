// Copyright 2015 Jacek Masiulaniec. All rights reserved.
// Use of this source code is governed by a free license that can be
// found in the LICENSE file.

// Package riemann implements a Riemann client.
package riemann

import (
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
	"time"

	riemann "github.com/masiulaniec/riemann/proto"

	"github.com/golang/protobuf/proto"
)

const (
	frameMaxEvents  = 100    // how frequently to write?
	poolDefaultSize = 1      // how many parallel conns?
	poolMaxQueue    = 100000 // sender-side safety buffer
)

// errClosed is used internally to signal connection close.
var errClosed = errors.New("cclient closed")

// Event represents a Riemann event.
type Event struct {
	Host    string  // sending host
	Service string  // the service the event pertains to
	IsFloat bool    // value type
	Float   float64 // value if IsFloat
	Int     int64   // value if !IsFloat

	// Optional
	Description string   // arbitrary text
	Time        int64    // creation time (unix epoch)
	TTL         float32  // time to live (seconds)
	State       string   // associated health state
	Tags        []string // list of string labels
	Attributes  []string // list of key, value pairs
}

// ClientConfig can be used to override Client settings.
type ClientConfig struct {
	// PoolSize defines how many TCP connections to create.
	// Default: 1
	PoolSize int
}

// Client represents a Riemann client.
type Client struct {
	addr string
	pool chan *Event
}

// NewClient returns a client for Riemann server listening at
// the given address. If nil config is provided, default settings
// are used.
func NewClient(addr string, config *ClientConfig) *Client {
	if config == nil {
		config = &ClientConfig{}
	}
	if config.PoolSize == 0 {
		config.PoolSize = poolDefaultSize
	}
	client := &Client{
		addr: addr,
		pool: make(chan *Event, poolMaxQueue),
	}
	for i := 0; i < config.PoolSize; i++ {
		go client.conn()
	}
	return client
}

// Close terminates the client by closing its connection pool.
func (c *Client) Close() error {
	close(c.pool)
	return nil
}

// conn keeps alive a connection to the server.
func (c *Client) conn() {
	for {
		err := c.conn1()
		if err == errClosed {
			return
		}
		log.Printf("riemann: server %s: %v", c.addr, err)
		time.Sleep(5 * time.Second)
	}
}

func (c *Client) conn1() error {
	addr := c.addr
	if !strings.HasSuffix(addr, ":5555") {
		addr += ":5555"
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	go c.readLoop(conn)
	return c.writeLoop(conn)
}

// writeLoop sends requests to the server.
func (c *Client) writeLoop(w io.Writer) error {
	frame := new(frame)
	frame.Reset()
	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-c.pool:
			if !ok {
				return errClosed
			}
			frame.append(event)
			if frame.Len == frameMaxEvents {
				if _, err := w.Write(frame.Bytes()); err != nil {
					return err
				}
				frame.Reset()
				timer.Reset()
			}
		case <-timer.C:
			if frame.Len > 0 {
				if _, err := w.Write(frame.Bytes()); err != nil {
					return err
				}
				frame.Reset()
			}
			timer.Reset()
		}
	}
}

// readLoop processes server responses.
func (c *Client) readLoop(r io.Reader) {
	// Just discard the acks. We know they are acks and not errors
	// because invalid messages are never transmitted. Instead,
	// bad data is detected eagerly in (*Client).Send, which helps
	// identify faulty call sites.
	io.Copy(ioutil.Discard, r)
}

// Send tries to delivier the given event. Delivery is not guaranteed.
// Send will panic if the given event is invalid.
func (c *Client) Send(event *Event) {
	if err := event.validate(); err != nil {
		log.Panic(err)
	}
	select {
	case c.pool <- event:
		// ok
	default:
		// Data loss.
	}
}

func (e *Event) validate() error {
	if e.Host == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return err
		}
		e.Host = hostname
	}
	if e.Time == 0 {
		now := time.Now().Unix()
		e.Time = now
	}
	if e.Service == "" {
		return errors.New("undefined service")
	}
	if e.IsFloat && e.Int != 0 {
		return errors.New("float event with non-float value")
	}
	return nil
}

// frame represents a Riemann frame.
type frame struct {
	proto.Buffer               // holds Riemann header and protobuf body
	Len          int           // number of events buffered
	event        riemann.Event // helps avoid an allocation
	scratch      [512]byte     // helps avoid an allocation
}

// Reset prepares a new frame.
func (fr *frame) Reset() {
	fr.Buffer.Reset()
	// Reserve space for Riemann's frame header.
	fr.EncodeFixed32(0)
	fr.Len = 0
}

// Bytes returns the frame in a ready-to-write form.
func (fr *frame) Bytes() []byte {
	var (
		header = fr.Buffer.Bytes()[:4]
		body   = fr.Buffer.Bytes()[4:]
	)
	bodyLen := uint32(len(body))
	header[0] = byte(bodyLen >> 24)
	header[1] = byte(bodyLen >> 16)
	header[2] = byte(bodyLen >> 8)
	header[3] = byte(bodyLen >> 0)
	return fr.Buffer.Bytes()
}

// append encodes the given event in the frame.
func (fr *frame) append(event *Event) {
	e := &fr.event

	// Prepare the event for marshaling.
	e.Time = &event.Time
	if event.State != "" {
		e.State = &event.State
	} else {
		e.State = nil
	}
	e.Service = &event.Service
	e.Host = &event.Host
	if event.Description != "" {
		e.Description = &event.Description
	} else {
		e.Description = nil
	}
	e.Tags = event.Tags
	if event.TTL != 0 {
		e.Ttl = &event.TTL
	} else {
		e.Ttl = nil
	}
	if event.IsFloat {
		e.MetricSint64 = nil
		e.MetricD = &event.Float
	} else {
		e.MetricSint64 = &event.Int
		e.MetricD = nil
	}
	e.Attributes = e.Attributes[:0]
	for i := 0; i < len(event.Attributes); i += 2 {
		attr := &riemann.Attribute{
			Key:   &event.Attributes[i],
			Value: &event.Attributes[i+1],
		}
		e.Attributes = append(e.Attributes, attr)
	}

	// Marshal it.
	ebuf := proto.NewBuffer(fr.scratch[:0])
	if err := ebuf.Marshal(e); err != nil {
		panic(err)
	}
	if err := fr.EncodeVarint(6<<3 | proto.WireBytes); err != nil {
		panic(err)
	}
	if err := fr.EncodeRawBytes(ebuf.Bytes()); err != nil {
		panic(err)
	}
	fr.Len++
}
