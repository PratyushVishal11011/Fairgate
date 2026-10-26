package deduplication

import (
	"FairGate/internal/wire"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

var (
	ErrConflict = errors.New("event identity reused with different contents")
	ErrInvalid  = errors.New("event is missing producer identity")
)

type eventKey struct {
	ProducerID    string
	ProducerEpoch string
	Seq           uint64
	Idx           uint16
}

type eventContent struct {
	EventTime time.Time `json:"event_time"`
	EventType string    `json:"event_type"`
	Payload   string    `json:"payload"`
}

type Manager struct {
	mu     sync.Mutex
	events map[eventKey][sha256.Size]byte
	//identities whose persist is running; closed when it finishes.
	inflight map[eventKey]chan struct{}
}

func NewManager(recovered []wire.Event) (*Manager, error) {
	m := &Manager{
		events:   make(map[eventKey][sha256.Size]byte, len(recovered)),
		inflight: make(map[eventKey]chan struct{}),
	}

	for _, event := range recovered {
		key, fingerprint, err := identify(event)
		if err != nil {
			return nil, err
		}

		if existing, ok := m.events[key]; ok {
			if existing != fingerprint {
				return nil, ErrConflict
			}
			continue
		}

		m.events[key] = fingerprint
	}

	return m, nil
}

// PersistIfNew persists a new event before recording its identity.
// Concurrent retries of one identity are serialized, but the manager mutex is not held
// during persist, so different identities persist concurrently and share WAL group commits.
func (m *Manager) PersistIfNew(
	event wire.Event,
	persist func() error,
) (duplicate bool, err error) {
	key, fingerprint, err := identify(event)
	if err != nil {
		return false, err
	}

	m.mu.Lock()
	for {
		if existing, ok := m.events[key]; ok {
			m.mu.Unlock()
			if existing != fingerprint {
				return false, ErrConflict
			}
			return true, nil
		}

		//another handler is persisting this identity; wait for it, then check again.
		//if its persist failed, the identity is still unknown and this call persists it.
		done, ok := m.inflight[key]
		if !ok {
			break
		}
		m.mu.Unlock()
		<-done
		m.mu.Lock()
	}

	done := make(chan struct{})
	m.inflight[key] = done
	m.mu.Unlock()

	err = persist()

	m.mu.Lock()
	if err == nil {
		m.events[key] = fingerprint
	}
	delete(m.inflight, key)
	m.mu.Unlock()
	close(done)

	if err != nil {
		return false, err
	}
	return false, nil
}

func identify(event wire.Event) (eventKey, [sha256.Size]byte, error) {
	key := eventKey{
		ProducerID:    event.ProducerId,
		ProducerEpoch: event.ProducerEpoch,
		Seq:           event.Seq,
		Idx:           event.Idx,
	}

	if key.ProducerID == "" || key.ProducerEpoch == "" {
		return key, [sha256.Size]byte{}, ErrInvalid
	}

	content := eventContent{
		EventTime: event.EventTime.UTC(),
		EventType: event.EventType,
		Payload:   event.Payload,
	}

	encoded, err := json.Marshal(content)
	if err != nil {
		return key, [sha256.Size]byte{}, err
	}

	return key, sha256.Sum256(encoded), nil
}
