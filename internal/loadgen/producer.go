package loadgen

import (
	"FairGate/internal/wire"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"time"
)

// ACK statuses and messages sent by the gateway; see docs/wire-protocol.md.
const (
	ackAccepted      = "accepted"
	ackRejected      = "rejected"
	ackInvalid       = "invalid"
	duplicateMessage = "duplicate_event_already_persisted"
)

const baseBackoff = 10 * time.Millisecond

// pending is an event that has been generated but not yet resolved by an ACK.
// Its encoded frame is kept so every retry is byte-identical, which the gateway's
// deduplication requires to acknowledge it as a duplicate.
type pending struct {
	seq       uint64
	data      []byte
	malformed bool
	firstSent time.Time
	sentAt    time.Time
}

// sample is one accepted event: when its ACK arrived and how long it took.
type sample struct {
	atMs uint32
	//from the last send to the accepted ACK.
	ackUs uint32
	//from the first send to the accepted ACK, including retries.
	deliveryUs uint32
}

type bucket struct {
	sent, accepted, rejected, invalid, errors int
}

type connection struct {
	conn   net.Conn
	w      *bufio.Writer
	acks   chan wire.Ack
	errs   chan error
	closed chan struct{}
}

func (c *connection) read() {
	r := bufio.NewReader(c.conn)
	for {
		frame, err := wire.ReadFrame(r)
		if err != nil {
			c.errs <- err
			return
		}
		if frame.Type != wire.FrameTypeAck {
			continue
		}
		var ack wire.Ack
		if err := json.Unmarshal(frame.Payload, &ack); err != nil {
			c.errs <- fmt.Errorf("decode ack: %w", err)
			return
		}
		select {
		case c.acks <- ack:
		case <-c.closed:
			return
		}
	}
}

func (c *connection) close() {
	close(c.closed)
	_ = c.conn.Close()
}

type producer struct {
	cfg    Config
	id     string
	hot    bool
	rate   float64
	window int
	delay  time.Duration
	start  time.Time
	rng    *rand.Rand

	nextSeq  uint64
	outcomes []Outcome
	retry    []*pending
	inflight []*pending

	conn         *connection
	backoffUntil time.Time
	rejectStreak int
	failStreak   int
	nextSend     time.Time

	result  ProducerResult
	samples []sample
	buckets []bucket
}

func newProducer(cfg Config, i int, start time.Time) *producer {
	var delay time.Duration
	if cfg.Producers > 1 {
		delay = cfg.Ramp * time.Duration(i) / time.Duration(cfg.Producers)
	}
	id := cfg.producerID(i)
	return &producer{
		cfg:      cfg,
		id:       id,
		hot:      cfg.isHot(i),
		rate:     cfg.rateFor(i),
		window:   cfg.windowFor(i),
		delay:    delay,
		start:    start,
		rng:      rand.New(rand.NewSource(cfg.Seed + int64(i))),
		outcomes: []Outcome{OutcomeNotSent},
		result: ProducerResult{
			ID:       id,
			Hot:      cfg.isHot(i),
			Rejected: map[string]int{},
			Invalid:  map[string]int{},
			Errors:   map[string]int{},
		},
	}
}

// run sends events until sendCtx is done or the producer has sent all of its events,
// then keeps retrying outstanding events until they resolve or the drain timeout passes.
func (p *producer) run(sendCtx context.Context) {
	defer p.finish()

	if p.delay > 0 {
		timer := time.NewTimer(p.delay)
		select {
		case <-timer.C:
		case <-sendCtx.Done():
			timer.Stop()
			return
		}
	}

	draining := false
	var drainDeadline time.Time
	for {
		now := time.Now()
		if !draining && (sendCtx.Err() != nil || p.exhausted()) {
			draining = true
			drainDeadline = now.Add(p.cfg.DrainTimeout)
		}
		if draining && (!p.outstanding() || !now.Before(drainDeadline)) {
			return
		}

		if p.conn == nil {
			if now.Before(p.backoffUntil) {
				p.sleep(sendCtx, draining, p.backoffUntil, drainDeadline)
				continue
			}
			if err := p.connect(); err != nil {
				p.fail(err)
				continue
			}
		}

		if err := p.fill(draining); err != nil {
			p.fail(err)
			continue
		}

		timer := time.NewTimer(time.Until(p.nextWake(draining, drainDeadline)))
		var stop <-chan struct{}
		if !draining {
			stop = sendCtx.Done()
		}
		select {
		case ack := <-p.conn.acks:
			p.handleAck(ack)
		case err := <-p.conn.errs:
			p.fail(err)
		case <-timer.C:
		case <-stop:
		}
		timer.Stop()

		if p.conn != nil && len(p.inflight) > 0 && time.Since(p.inflight[0].sentAt) >= p.cfg.AckTimeout {
			p.fail(errors.New("ack timeout"))
		}
	}
}

func (p *producer) exhausted() bool {
	return p.cfg.Events > 0 && p.nextSeq >= uint64(p.cfg.Events)
}

// outstanding reports whether any real event still waits for an accepted or invalid ACK.
func (p *producer) outstanding() bool {
	if len(p.retry) > 0 {
		return true
	}
	for _, ev := range p.inflight {
		if !ev.malformed {
			return true
		}
	}
	return false
}

func (p *producer) sleep(sendCtx context.Context, draining bool, until, drainDeadline time.Time) {
	if draining && drainDeadline.Before(until) {
		until = drainDeadline
	}
	timer := time.NewTimer(time.Until(until))
	defer timer.Stop()
	var stop <-chan struct{}
	if !draining {
		stop = sendCtx.Done()
	}
	select {
	case <-timer.C:
	case <-stop:
	}
}

func (p *producer) connect() error {
	dialer := net.Dialer{Timeout: p.cfg.DialTimeout}
	conn, err := dialer.Dial("tcp", p.cfg.Addr)
	if err != nil {
		return err
	}
	if !p.cfg.Legacy {
		_ = conn.SetDeadline(time.Now().Add(p.cfg.DialTimeout))
		if _, err := wire.Handshake(conn, "fairgate-loadgen"); err != nil {
			_ = conn.Close()
			return fmt.Errorf("handshake: %w", err)
		}
		_ = conn.SetDeadline(time.Time{})
	}
	p.conn = &connection{
		conn:   conn,
		w:      bufio.NewWriter(conn),
		acks:   make(chan wire.Ack, p.window+1),
		errs:   make(chan error, 1),
		closed: make(chan struct{}),
	}
	go p.conn.read()
	return nil
}

// fail drops the connection after a network error and requeues every in-flight event.
// The gateway may already have persisted some of them; resending is safe because
// the identity and content are unchanged.
func (p *producer) fail(err error) {
	p.result.LastError = err.Error()
	p.failStreak++
	p.backoffUntil = time.Now().Add(p.backoff(p.failStreak))
	if p.conn == nil {
		p.result.DialFailures++
		return
	}
	p.result.Disconnects++
	p.conn.close()
	p.conn = nil
	requeue := make([]*pending, 0, len(p.inflight)+len(p.retry))
	for _, ev := range p.inflight {
		if !ev.malformed {
			requeue = append(requeue, ev)
		}
	}
	p.retry = append(requeue, p.retry...)
	p.inflight = nil
}

func (p *producer) backoff(streak int) time.Duration {
	delay := p.cfg.MaxBackoff
	if shift := streak - 1; shift < 16 {
		delay = min(baseBackoff<<shift, p.cfg.MaxBackoff)
	}
	//±20% jitter keeps producers that were refused together from retrying together.
	return time.Duration(float64(delay) * (0.8 + 0.4*p.rng.Float64()))
}

// fill writes events until the window is full, the producer is backing off, or pacing says wait.
func (p *producer) fill(draining bool) error {
	now := time.Now()
	if now.Before(p.backoffUntil) {
		return nil
	}
	if err := p.conn.conn.SetWriteDeadline(now.Add(p.cfg.AckTimeout)); err != nil {
		return err
	}
	for len(p.inflight) < p.window {
		ev, fresh := p.next(draining, now)
		if ev == nil {
			break
		}
		if err := p.write(ev, now); err != nil {
			return err
		}
		if fresh && p.cfg.MalformedRatio > 0 && len(p.inflight) < p.window && p.rng.Float64() < p.cfg.MalformedRatio {
			bad := &pending{data: []byte(`{"producer_id":`), malformed: true}
			if err := p.write(bad, now); err != nil {
				return err
			}
		}
	}
	return p.conn.w.Flush()
}

func (p *producer) write(ev *pending, now time.Time) error {
	// Count the event as in flight before the write: if the write fails partway,
	// the gateway may still have received it, and fail() must requeue it.
	if ev.firstSent.IsZero() {
		ev.firstSent = now
	}
	ev.sentAt = now
	p.inflight = append(p.inflight, ev)
	if ev.malformed {
		p.result.Malformed++
	} else {
		p.result.Sent++
		p.bucket(now).sent++
		if p.result.FirstSend.IsZero() {
			p.result.FirstSend = now
		}
	}
	return wire.WriteFrame(p.conn.w, wire.Frame{Type: wire.FrameTypeEvent, Payload: ev.data})
}

// next returns the next event to send: a retry first, otherwise a new event if pacing allows.
func (p *producer) next(draining bool, now time.Time) (*pending, bool) {
	if len(p.retry) > 0 {
		ev := p.retry[0]
		p.retry = p.retry[1:]
		return ev, false
	}
	if draining || p.exhausted() {
		return nil, false
	}
	if p.rate > 0 {
		if now.Before(p.nextSend) {
			return nil, false
		}
		//a producer that fell behind its schedule does not burst to catch up by more than a second.
		if p.nextSend.Before(now.Add(-time.Second)) {
			p.nextSend = now
		}
		p.nextSend = p.nextSend.Add(time.Duration(float64(time.Second) / p.rate))
	}

	p.nextSeq++
	seq := p.nextSeq
	data, err := json.Marshal(wire.Event{
		ProducerId:    p.id,
		ProducerEpoch: p.cfg.Epoch,
		EventTime:     now.UTC().Truncate(time.Millisecond),
		Seq:           seq,
		EventType:     p.cfg.EventType,
		Payload:       Payload(p.id, p.cfg.Epoch, seq, p.cfg.PayloadBytes),
	})
	if err != nil {
		//a wire.Event of strings, numbers, and a UTC time always encodes.
		panic(err)
	}
	p.outcomes = append(p.outcomes, OutcomeUnacked)
	p.result.Events++
	return &pending{seq: seq, data: data}, true
}

func (p *producer) nextWake(draining bool, drainDeadline time.Time) time.Time {
	now := time.Now()
	wake := now.Add(time.Minute)
	earliest := func(t time.Time) {
		if t.Before(wake) {
			wake = t
		}
	}
	if len(p.inflight) > 0 {
		earliest(p.inflight[0].sentAt.Add(p.cfg.AckTimeout))
	}
	if now.Before(p.backoffUntil) {
		earliest(p.backoffUntil)
	}
	if p.rate > 0 && !draining && !p.exhausted() && len(p.retry) == 0 && len(p.inflight) < p.window {
		earliest(p.nextSend)
	}
	if draining {
		earliest(drainDeadline)
	}
	return wake
}

func (p *producer) handleAck(ack wire.Ack) {
	now := time.Now()

	//the gateway handles a connection's frames in order, so an ACK answers the oldest
	//in-flight frame. An ACK without an identity answers a frame that failed to decode.
	pos := -1
	if ack.ProducerId == "" {
		if len(p.inflight) > 0 {
			pos = 0
		}
	} else {
		for i, ev := range p.inflight {
			if !ev.malformed && ack.ProducerId == p.id && ev.seq == ack.Seq && ack.Idx == 0 {
				pos = i
				break
			}
		}
	}
	if pos < 0 {
		p.result.UnmatchedAcks++
		return
	}

	ev := p.inflight[pos]
	//frames ahead of the answered one got no ACK; retry them.
	for _, skipped := range p.inflight[:pos] {
		if !skipped.malformed {
			p.retry = append(p.retry, skipped)
		}
	}
	p.inflight = p.inflight[pos+1:]

	if ev.malformed {
		if ack.Status == ackInvalid {
			p.result.MalformedRejected++
		} else {
			p.result.UnmatchedAcks++
		}
		return
	}

	b := p.bucket(now)
	switch ack.Status {
	case ackAccepted:
		p.outcomes[ev.seq] = OutcomeAccepted
		p.result.Accepted++
		if ack.Message == duplicateMessage {
			p.result.Duplicates++
		}
		p.result.LastAck = now
		p.samples = append(p.samples, sample{
			atMs:       clampU32(now.Sub(p.start) / time.Millisecond),
			ackUs:      clampU32(now.Sub(ev.sentAt) / time.Microsecond),
			deliveryUs: clampU32(now.Sub(ev.firstSent) / time.Microsecond),
		})
		b.accepted++
		p.rejectStreak = 0
		p.failStreak = 0
	case ackRejected:
		p.result.Rejected[ack.Message]++
		b.rejected++
		p.retry = append(p.retry, ev)
		p.rejectStreak++
		p.backoffUntil = now.Add(p.backoff(p.rejectStreak))
	case ackInvalid:
		p.outcomes[ev.seq] = OutcomeInvalid
		p.result.Invalid[ack.Message]++
		b.invalid++
	default:
		//"error" and anything unknown are server-side failures: retry.
		p.result.Errors[ack.Status+":"+ack.Message]++
		b.errors++
		p.retry = append(p.retry, ev)
	}
}

func (p *producer) bucket(now time.Time) *bucket {
	second := max(int(now.Sub(p.start)/time.Second), 0)
	for len(p.buckets) <= second {
		p.buckets = append(p.buckets, bucket{})
	}
	return &p.buckets[second]
}

func (p *producer) finish() {
	if p.conn != nil {
		p.conn.close()
		p.conn = nil
	}
	for _, outcome := range p.outcomes[1:] {
		if outcome == OutcomeUnacked {
			p.result.Unacked++
		}
	}
	p.result.outcomes = p.outcomes
	p.result.samples = p.samples
	p.result.buckets = p.buckets
}

func clampU32(d time.Duration) uint32 {
	if d < 0 {
		return 0
	}
	if d > 1<<32-1 {
		return 1<<32 - 1
	}
	return uint32(d)
}
