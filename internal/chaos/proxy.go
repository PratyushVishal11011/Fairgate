// Package chaos injects faults around a FairGate deployment and checks that no accepted event is lost.
//
// Proxy is a TCP proxy that adds latency, fragments writes, resets connections, and partitions
// traffic. Scenario runs the load generator while timed steps inject faults (killing containers,
// stopping ClickHouse, changing proxy faults), then reconciles the run against storage.
package chaos

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Faults are applied to every proxied connection. The zero value forwards traffic unchanged.
type Faults struct {
	//delay added to every chunk of data, in each direction; order is preserved.
	Latency time.Duration `yaml:"latency"`
	//random extra delay of up to Jitter per chunk.
	Jitter time.Duration `yaml:"jitter"`
	//probability that a chunk resets its connection (RST to both ends) instead of being forwarded.
	ResetProbability float64 `yaml:"reset_probability"`
	//split forwarded data into writes of at most this many bytes; 0 forwards chunks whole.
	Fragment int `yaml:"fragment"`
	//pause between fragments, so the receiver sees them as separate reads instead of one coalesced read.
	FragmentDelay time.Duration `yaml:"fragment_delay"`
	//hold all data in both directions, as a network partition does, until cleared.
	Blackhole bool `yaml:"blackhole"`
	//reset new connections as soon as they are accepted.
	Refuse bool `yaml:"refuse"`
}

func (f Faults) Validate() error {
	switch {
	case f.Latency < 0 || f.Jitter < 0 || f.FragmentDelay < 0:
		return errors.New("latency, jitter, and fragment_delay cannot be negative")
	case f.ResetProbability < 0 || f.ResetProbability > 1:
		return errors.New("reset_probability must be between 0 and 1")
	case f.Fragment < 0:
		return errors.New("fragment cannot be negative")
	}
	return nil
}

type faultsJSON struct {
	Latency          string  `json:"latency,omitempty"`
	Jitter           string  `json:"jitter,omitempty"`
	ResetProbability float64 `json:"reset_probability,omitempty"`
	Fragment         int     `json:"fragment,omitempty"`
	FragmentDelay    string  `json:"fragment_delay,omitempty"`
	Blackhole        bool    `json:"blackhole,omitempty"`
	Refuse           bool    `json:"refuse,omitempty"`
}

// MarshalJSON writes durations as strings such as "50ms".
func (f Faults) MarshalJSON() ([]byte, error) {
	out := faultsJSON{ResetProbability: f.ResetProbability, Fragment: f.Fragment, Blackhole: f.Blackhole, Refuse: f.Refuse}
	if f.Latency > 0 {
		out.Latency = f.Latency.String()
	}
	if f.Jitter > 0 {
		out.Jitter = f.Jitter.String()
	}
	if f.FragmentDelay > 0 {
		out.FragmentDelay = f.FragmentDelay.String()
	}
	return json.Marshal(out)
}

func (f *Faults) UnmarshalJSON(data []byte) error {
	var in faultsJSON
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	parsed := Faults{ResetProbability: in.ResetProbability, Fragment: in.Fragment, Blackhole: in.Blackhole, Refuse: in.Refuse}
	for _, d := range []struct {
		src string
		dst *time.Duration
	}{{in.Latency, &parsed.Latency}, {in.Jitter, &parsed.Jitter}, {in.FragmentDelay, &parsed.FragmentDelay}} {
		if d.src == "" {
			continue
		}
		value, err := time.ParseDuration(d.src)
		if err != nil {
			return fmt.Errorf("parse duration %q: %w", d.src, err)
		}
		*d.dst = value
	}
	*f = parsed
	return nil
}

// Stats counts what the proxy has done since it started.
type Stats struct {
	Accepted  int64 `json:"accepted"`
	Refused   int64 `json:"refused"`
	Active    int   `json:"active"`
	Resets    int64 `json:"resets"`
	BytesUp   int64 `json:"bytes_up"`
	BytesDown int64 `json:"bytes_down"`
}

type Proxy struct {
	listener net.Listener
	upstream string
	faults   atomic.Pointer[Faults]

	mu    sync.Mutex
	links map[*link]struct{}

	rngMu sync.Mutex
	rng   *rand.Rand

	accepted, refused, resets, bytesUp, bytesDown atomic.Int64
	wg                                            sync.WaitGroup
}

// Listen starts a proxy on addr that forwards each connection to upstream.
func Listen(addr, upstream string, faults Faults) (*Proxy, error) {
	if err := faults.Validate(); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	p := &Proxy{
		listener: listener,
		upstream: upstream,
		links:    make(map[*link]struct{}),
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	p.faults.Store(&faults)
	p.wg.Add(1)
	go p.accept()
	return p, nil
}

func (p *Proxy) Addr() string {
	return p.listener.Addr().String()
}

func (p *Proxy) Faults() Faults {
	return *p.faults.Load()
}

// SetFaults replaces the active faults; data already queued keeps the delay it was given.
func (p *Proxy) SetFaults(f Faults) error {
	if err := f.Validate(); err != nil {
		return err
	}
	p.faults.Store(&f)
	return nil
}

// DisconnectAll resets every open connection and returns how many there were.
func (p *Proxy) DisconnectAll() int {
	p.mu.Lock()
	links := make([]*link, 0, len(p.links))
	for l := range p.links {
		links = append(links, l)
	}
	p.mu.Unlock()
	for _, l := range links {
		p.resets.Add(1)
		l.reset()
	}
	return len(links)
}

func (p *Proxy) Stats() Stats {
	p.mu.Lock()
	active := len(p.links)
	p.mu.Unlock()
	return Stats{
		Accepted:  p.accepted.Load(),
		Refused:   p.refused.Load(),
		Active:    active,
		Resets:    p.resets.Load(),
		BytesUp:   p.bytesUp.Load(),
		BytesDown: p.bytesDown.Load(),
	}
}

// Close stops accepting, resets every open connection, and waits for them to finish.
func (p *Proxy) Close() error {
	err := p.listener.Close()
	p.DisconnectAll()
	p.wg.Wait()
	return err
}

func (p *Proxy) accept() {
	defer p.wg.Done()
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		if p.Faults().Refuse {
			p.refused.Add(1)
			resetConn(client)
			continue
		}
		p.wg.Add(1)
		go p.serve(client)
	}
}

func (p *Proxy) serve(client net.Conn) {
	defer p.wg.Done()
	upstream, err := net.DialTimeout("tcp", p.upstream, 5*time.Second)
	if err != nil {
		p.refused.Add(1)
		resetConn(client)
		return
	}
	l := &link{client: client, upstream: upstream, closed: make(chan struct{})}
	p.mu.Lock()
	p.links[l] = struct{}{}
	p.mu.Unlock()
	p.accepted.Add(1)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		p.pipe(l, client, upstream, &p.bytesUp)
	}()
	go func() {
		defer wg.Done()
		p.pipe(l, upstream, client, &p.bytesDown)
	}()
	wg.Wait()
	l.close()

	p.mu.Lock()
	delete(p.links, l)
	p.mu.Unlock()
}

type chunk struct {
	data []byte
	due  time.Time
}

// pipe copies src to dst through a queue, so latency delays each chunk without
// limiting throughput to one chunk per delay.
func (p *Proxy) pipe(l *link, src, dst net.Conn, counter *atomic.Int64) {
	chunks := make(chan chunk, 64)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		failed := false
		for c := range chunks {
			//keep draining after a failure so the reader never blocks on a full queue.
			if failed || !l.waitUntil(c.due) || !p.waitWhileBlackholed(l) || p.write(l, dst, c.data) != nil {
				failed = true
				l.close()
			}
		}
		if !failed {
			if tcp, ok := dst.(*net.TCPConn); ok {
				_ = tcp.CloseWrite()
			}
		}
	}()

	buf := make([]byte, 32<<10)
	var lastDue time.Time
	for {
		n, err := src.Read(buf)
		if n > 0 {
			f := p.Faults()
			if f.ResetProbability > 0 && p.chance(f.ResetProbability) {
				p.resets.Add(1)
				l.reset()
				break
			}
			due := time.Now().Add(f.Latency)
			if f.Jitter > 0 {
				due = due.Add(time.Duration(p.int63n(int64(f.Jitter) + 1)))
			}
			//jitter never reorders data within a connection.
			if due.Before(lastDue) {
				due = lastDue
			}
			lastDue = due
			counter.Add(int64(n))
			chunks <- chunk{data: append([]byte(nil), buf[:n]...), due: due}
		}
		if err != nil {
			break
		}
	}
	close(chunks)
	<-writerDone
}

func (p *Proxy) write(l *link, dst net.Conn, data []byte) error {
	f := p.Faults()
	size := f.Fragment
	if size <= 0 {
		size = len(data)
	}
	for first := true; len(data) > 0; first = false {
		if !first && f.FragmentDelay > 0 && !l.waitUntil(time.Now().Add(f.FragmentDelay)) {
			return net.ErrClosed
		}
		n := min(size, len(data))
		if _, err := dst.Write(data[:n]); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func (p *Proxy) waitWhileBlackholed(l *link) bool {
	for p.Faults().Blackhole {
		if !l.waitUntil(time.Now().Add(20 * time.Millisecond)) {
			return false
		}
	}
	return true
}

func (p *Proxy) chance(probability float64) bool {
	p.rngMu.Lock()
	defer p.rngMu.Unlock()
	return p.rng.Float64() < probability
}

func (p *Proxy) int63n(n int64) int64 {
	p.rngMu.Lock()
	defer p.rngMu.Unlock()
	return p.rng.Int63n(n)
}

type link struct {
	client, upstream net.Conn
	once             sync.Once
	closed           chan struct{}
}

func (l *link) close() {
	l.once.Do(func() {
		_ = l.client.Close()
		_ = l.upstream.Close()
		close(l.closed)
	})
}

// reset closes both ends with an RST rather than a FIN, as a crashed peer or middlebox would.
func (l *link) reset() {
	l.once.Do(func() {
		resetConn(l.client)
		resetConn(l.upstream)
		close(l.closed)
	})
}

// waitUntil sleeps until t and reports false if the link closed first.
func (l *link) waitUntil(t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		select {
		case <-l.closed:
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-l.closed:
		return false
	}
}

func resetConn(conn net.Conn) {
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}
	_ = conn.Close()
}
