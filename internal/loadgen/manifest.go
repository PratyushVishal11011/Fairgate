package loadgen

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// ManifestVersion is bumped when the manifest format changes incompatibly.
const ManifestVersion = 1

// Manifest records the outcome of every event a run sent, for reconciliation against storage.
// Every event identity is (producer ID, Epoch, seq, idx 0); seq starts at 1.
type Manifest struct {
	Version      int                `json:"version"`
	GeneratedAt  time.Time          `json:"generated_at"`
	Epoch        string             `json:"epoch"`
	EventType    string             `json:"event_type"`
	PayloadBytes int                `json:"payload_bytes"`
	Producers    []ManifestProducer `json:"producers"`
}

type ManifestProducer struct {
	ID string `json:"id"`
	//events the gateway acknowledged as accepted: each must be stored exactly once.
	Accepted Ranges `json:"accepted"`
	//events sent but never acknowledged as accepted: each may be stored at most once.
	Unacked Ranges `json:"unacked,omitempty"`
	//events the gateway refused as invalid: none may be stored.
	Invalid Ranges `json:"invalid,omitempty"`
}

// Outcome classifies one event of a run.
type Outcome uint8

const (
	//never sent; no seq at or after the producer's last sent event is stored.
	OutcomeNotSent Outcome = iota
	OutcomeUnacked
	OutcomeAccepted
	OutcomeInvalid
)

func (o Outcome) String() string {
	switch o {
	case OutcomeUnacked:
		return "unacked"
	case OutcomeAccepted:
		return "accepted"
	case OutcomeInvalid:
		return "invalid"
	default:
		return "not_sent"
	}
}

// Outcomes returns the producer's outcomes indexed by seq; index 0 is unused.
func (p ManifestProducer) Outcomes() []Outcome {
	var last uint64
	for _, ranges := range []Ranges{p.Accepted, p.Unacked, p.Invalid} {
		if len(ranges) > 0 {
			last = max(last, ranges[len(ranges)-1][1])
		}
	}
	outcomes := make([]Outcome, last+1)
	mark := func(ranges Ranges, outcome Outcome) {
		for _, r := range ranges {
			for seq := r[0]; seq <= r[1]; seq++ {
				outcomes[seq] = outcome
			}
		}
	}
	mark(p.Accepted, OutcomeAccepted)
	mark(p.Unacked, OutcomeUnacked)
	mark(p.Invalid, OutcomeInvalid)
	return outcomes
}

// Count returns the number of events with each outcome across all producers.
func (m *Manifest) Count(outcome Outcome) uint64 {
	var total uint64
	for _, p := range m.Producers {
		switch outcome {
		case OutcomeAccepted:
			total += p.Accepted.Count()
		case OutcomeUnacked:
			total += p.Unacked.Count()
		case OutcomeInvalid:
			total += p.Invalid.Count()
		}
	}
	return total
}

// ProducerIDs returns the producer IDs in manifest order.
func (m *Manifest) ProducerIDs() []string {
	ids := make([]string, len(m.Producers))
	for i, p := range m.Producers {
		ids[i] = p.ID
	}
	return ids
}

func (m *Manifest) Validate() error {
	if m.Version != ManifestVersion {
		return fmt.Errorf("unsupported manifest version %d, want %d", m.Version, ManifestVersion)
	}
	if m.Epoch == "" {
		return fmt.Errorf("manifest has no epoch")
	}
	seen := make(map[string]struct{}, len(m.Producers))
	for _, p := range m.Producers {
		if p.ID == "" {
			return fmt.Errorf("manifest has a producer without an ID")
		}
		if _, ok := seen[p.ID]; ok {
			return fmt.Errorf("manifest lists producer %s twice", p.ID)
		}
		seen[p.ID] = struct{}{}
		for _, ranges := range []Ranges{p.Accepted, p.Unacked, p.Invalid} {
			if err := ranges.validate(); err != nil {
				return fmt.Errorf("producer %s: %w", p.ID, err)
			}
		}
	}
	return nil
}

func ReadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse manifest %s: %w", path, err)
	}
	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("manifest %s: %w", path, err)
	}
	return &m, nil
}

func WriteManifest(path string, m *Manifest) error {
	return writeJSON(path, m)
}

// Range is an inclusive [first, last] span of sequence numbers.
type Range [2]uint64

// Ranges is a sorted list of non-overlapping ranges.
type Ranges []Range

// RangesOf collects the seqs whose outcome is want into ranges.
func RangesOf(outcomes []Outcome, want Outcome) Ranges {
	var ranges Ranges
	for seq := 1; seq < len(outcomes); seq++ {
		if outcomes[seq] == want {
			ranges = ranges.Add(uint64(seq))
		}
	}
	return ranges
}

// Add appends seq, which must be greater than every seq already added.
func (r Ranges) Add(seq uint64) Ranges {
	if n := len(r); n > 0 && r[n-1][1]+1 == seq {
		r[n-1][1] = seq
		return r
	}
	return append(r, Range{seq, seq})
}

func (r Ranges) Count() uint64 {
	var total uint64
	for _, span := range r {
		total += span[1] - span[0] + 1
	}
	return total
}

// String formats the ranges as "1-5,9,12-13".
func (r Ranges) String() string {
	parts := make([]string, len(r))
	for i, span := range r {
		if span[0] == span[1] {
			parts[i] = strconv.FormatUint(span[0], 10)
		} else {
			parts[i] = fmt.Sprintf("%d-%d", span[0], span[1])
		}
	}
	return strings.Join(parts, ",")
}

func (r Ranges) validate() error {
	var prev uint64
	for i, span := range r {
		if span[0] == 0 || span[0] > span[1] {
			return fmt.Errorf("invalid range %d-%d", span[0], span[1])
		}
		if i > 0 && span[0] <= prev {
			return fmt.Errorf("ranges are not sorted and disjoint at %d-%d", span[0], span[1])
		}
		prev = span[1]
	}
	return nil
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}
