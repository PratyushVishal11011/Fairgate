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
}

func NewManager(recovered []wire.Event) (*Manager, error) {
	m := &Manager{
		events: make(map[eventKey][sha256.Size]byte, len(recovered)),
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
// Concurrent retries are serialized by the manager mutex.
func (m *Manager) PersistIfNew(
	event wire.Event,
	persist func() error,
) (duplicate bool, err error) {
	key, fingerprint, err := identify(event)
	if err != nil {
		return false, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.events[key]; ok {
		if existing != fingerprint {
			return false, ErrConflict
		}
		return true, nil
	}

	if err := persist(); err != nil {
		return false, err
	}

	m.events[key] = fingerprint
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
