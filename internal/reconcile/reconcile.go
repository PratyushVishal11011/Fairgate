// Package reconcile checks a load-generator manifest against what reached storage.
//
// Every event the gateway accepted must be stored exactly once with the content that was sent.
// Events whose outcome the generator never learned may be stored at most once, and events the
// gateway refused as invalid, or that were never sent, must not be stored at all.
package reconcile

import (
	"FairGate/internal/loadgen"
	"context"
	"fmt"
	"io"
	"slices"
	"time"
)

// Row is one stored event identity of a run.
type Row struct {
	ProducerID string
	Seq        uint64
	Idx        uint64
	//stored rows for this identity; more than one is a duplicate.
	Copies uint64
	//filled only when content is requested.
	EventType string
	Payload   string
	//distinct (event_type, payload) pairs across the copies; filled only when content is requested.
	Variants uint64
}

// Source reads the stored events of one run.
type Source interface {
	// Count returns the number of distinct identities stored for the epoch's producers.
	Count(ctx context.Context, epoch string, producers []string) (uint64, error)
	// Scan calls fn once per stored identity of the epoch's producers.
	Scan(ctx context.Context, epoch string, producers []string, content bool, fn func(Row) error) error
}

type Options struct {
	//how long to wait for accepted events to reach storage before reporting them missing.
	Timeout time.Duration
	//how often to check storage while waiting.
	PollInterval time.Duration
	//compare stored event_type and payload with what the generator sent.
	VerifyContent bool
	//progress messages while waiting; nil discards them.
	Logf func(format string, args ...any)
}

// Report is the result of a reconciliation, written as reconcile.json.
type Report struct {
	Epoch  string `json:"epoch"`
	Passed bool   `json:"passed"`

	Accepted uint64 `json:"accepted"`
	Unacked  uint64 `json:"unacked"`
	Invalid  uint64 `json:"invalid"`

	StoredIdentities uint64 `json:"stored_identities"`
	StoredRows       uint64 `json:"stored_rows"`
	//unacked events that were stored; allowed, since the ACK can be lost after the WAL append.
	StoredUnacked uint64 `json:"stored_unacked"`

	//violations
	Missing    uint64 `json:"missing"`
	Duplicated uint64 `json:"duplicated"`
	//rows beyond the first for duplicated identities.
	ExtraRows  uint64 `json:"extra_rows"`
	Unexpected uint64 `json:"unexpected"`
	Mismatched uint64 `json:"mismatched"`

	ContentVerified bool    `json:"content_verified"`
	Polls           int     `json:"polls"`
	WaitedSeconds   float64 `json:"waited_seconds"`
	//producers with at least one violation.
	Producers []ProducerReport `json:"producers,omitempty"`
}

type ProducerReport struct {
	ID string `json:"id"`
	//accepted but not stored.
	Missing loadgen.Ranges `json:"missing,omitempty"`
	//stored more than once.
	Duplicated loadgen.Ranges `json:"duplicated,omitempty"`
	//stored although never sent, or refused as invalid.
	Unexpected loadgen.Ranges `json:"unexpected,omitempty"`
	//stored with an idx other than 0, which the generator never sends.
	UnexpectedIdx int `json:"unexpected_idx,omitempty"`
	//stored with content other than what was sent.
	Mismatched loadgen.Ranges `json:"mismatched,omitempty"`
}

// Run waits until every accepted event is stored or opts.Timeout passes, then reports
// every identity that breaks the exactly-once contract.
func Run(ctx context.Context, src Source, m *loadgen.Manifest, opts Options) (*Report, error) {
	if opts.PollInterval <= 0 {
		opts.PollInterval = 2 * time.Second
	}
	logf := opts.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}

	producers := m.ProducerIDs()
	accepted := m.Count(loadgen.OutcomeAccepted)
	start := time.Now()
	deadline := start.Add(opts.Timeout)
	polls := 0

	for {
		polls++
		final := !time.Now().Before(deadline)

		//a distinct-count query is cheap; the full scan runs only once enough identities are stored.
		if !final {
			stored, err := src.Count(ctx, m.Epoch, producers)
			if err != nil {
				return nil, err
			}
			if stored < accepted {
				logf("waiting: %d of %d accepted events stored", stored, accepted)
				if err := sleep(ctx, min(opts.PollInterval, time.Until(deadline))); err != nil {
					return nil, err
				}
				continue
			}
		}

		report, err := check(ctx, src, m, opts.VerifyContent)
		if err != nil {
			return nil, err
		}
		report.Polls = polls
		report.WaitedSeconds = time.Since(start).Seconds()
		if report.Missing == 0 || final {
			return report, nil
		}
		logf("waiting: %d accepted events not stored yet", report.Missing)
		if err := sleep(ctx, min(opts.PollInterval, time.Until(deadline))); err != nil {
			return nil, err
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func check(ctx context.Context, src Source, m *loadgen.Manifest, verifyContent bool) (*Report, error) {
	report := &Report{
		Epoch:           m.Epoch,
		Accepted:        m.Count(loadgen.OutcomeAccepted),
		Unacked:         m.Count(loadgen.OutcomeUnacked),
		Invalid:         m.Count(loadgen.OutcomeInvalid),
		ContentVerified: verifyContent,
	}

	outcomes := make(map[string][]loadgen.Outcome, len(m.Producers))
	//copies[producer][seq] counts stored rows for accepted and unacked events.
	copies := make(map[string][]uint64, len(m.Producers))
	issues := make(map[string]*ProducerReport)
	issue := func(id string) *ProducerReport {
		if issues[id] == nil {
			issues[id] = &ProducerReport{ID: id}
		}
		return issues[id]
	}
	unexpected := make(map[string][]uint64)
	mismatched := make(map[string][]uint64)

	for _, p := range m.Producers {
		o := p.Outcomes()
		outcomes[p.ID] = o
		copies[p.ID] = make([]uint64, len(o))
	}

	err := src.Scan(ctx, m.Epoch, m.ProducerIDs(), verifyContent, func(row Row) error {
		report.StoredIdentities++
		report.StoredRows += row.Copies
		o, ok := outcomes[row.ProducerID]
		if !ok {
			return fmt.Errorf("storage returned producer %q, which is not in the manifest", row.ProducerID)
		}
		if row.Idx != 0 {
			issue(row.ProducerID).UnexpectedIdx++
			report.Unexpected++
			return nil
		}
		if row.Seq >= uint64(len(o)) || o[row.Seq] == loadgen.OutcomeNotSent || o[row.Seq] == loadgen.OutcomeInvalid {
			unexpected[row.ProducerID] = append(unexpected[row.ProducerID], row.Seq)
			return nil
		}
		copies[row.ProducerID][row.Seq] = row.Copies
		if verifyContent {
			want := loadgen.Payload(row.ProducerID, m.Epoch, row.Seq, m.PayloadBytes)
			if row.Variants > 1 || row.EventType != m.EventType || row.Payload != want {
				mismatched[row.ProducerID] = append(mismatched[row.ProducerID], row.Seq)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	for _, p := range m.Producers {
		o := outcomes[p.ID]
		c := copies[p.ID]
		var missing, duplicated loadgen.Ranges
		for seq := 1; seq < len(o); seq++ {
			switch {
			case o[seq] == loadgen.OutcomeAccepted && c[seq] == 0:
				missing = missing.Add(uint64(seq))
			case o[seq] == loadgen.OutcomeUnacked && c[seq] > 0:
				report.StoredUnacked++
			}
			if c[seq] > 1 {
				duplicated = duplicated.Add(uint64(seq))
				report.ExtraRows += c[seq] - 1
			}
		}
		report.Missing += missing.Count()
		report.Duplicated += duplicated.Count()
		if len(missing) > 0 {
			issue(p.ID).Missing = missing
		}
		if len(duplicated) > 0 {
			issue(p.ID).Duplicated = duplicated
		}
		if seqs := unexpected[p.ID]; len(seqs) > 0 {
			issue(p.ID).Unexpected = rangesOf(seqs)
			report.Unexpected += uint64(len(seqs))
		}
		if seqs := mismatched[p.ID]; len(seqs) > 0 {
			issue(p.ID).Mismatched = rangesOf(seqs)
			report.Mismatched += uint64(len(seqs))
		}
		if r := issues[p.ID]; r != nil {
			report.Producers = append(report.Producers, *r)
		}
	}

	report.Passed = report.Missing == 0 && report.Duplicated == 0 && report.Unexpected == 0 && report.Mismatched == 0
	return report, nil
}

// rangesOf sorts seqs and collapses them into ranges.
func rangesOf(seqs []uint64) loadgen.Ranges {
	sorted := append([]uint64(nil), seqs...)
	slices.Sort(sorted)
	var ranges loadgen.Ranges
	for i, seq := range sorted {
		if i > 0 && seq == sorted[i-1] {
			continue
		}
		ranges = ranges.Add(seq)
	}
	return ranges
}

// WriteText prints a short human-readable report.
func (r *Report) WriteText(w io.Writer) {
	verdict := "PASS"
	if !r.Passed {
		verdict = "FAIL"
	}
	fmt.Fprintf(w, "reconcile %s: epoch %s, waited %.1fs over %d polls\n", verdict, r.Epoch, r.WaitedSeconds, r.Polls)
	fmt.Fprintf(w, "  manifest  %d accepted, %d unacked, %d invalid\n", r.Accepted, r.Unacked, r.Invalid)
	fmt.Fprintf(w, "  stored    %d identities in %d rows (%d of them unacked, which is allowed)\n",
		r.StoredIdentities, r.StoredRows, r.StoredUnacked)
	fmt.Fprintf(w, "  missing   %d accepted events not stored\n", r.Missing)
	fmt.Fprintf(w, "  duplicate %d identities stored more than once (%d extra rows)\n", r.Duplicated, r.ExtraRows)
	fmt.Fprintf(w, "  unexpected %d stored events that were never sent or were refused\n", r.Unexpected)
	if r.ContentVerified {
		fmt.Fprintf(w, "  mismatch  %d stored events with content other than what was sent\n", r.Mismatched)
	}
	const shown = 10
	for i, p := range r.Producers {
		if i == shown {
			fmt.Fprintf(w, "  ... %d more producers with violations in the JSON report\n", len(r.Producers)-shown)
			break
		}
		fmt.Fprintf(w, "  %s:", p.ID)
		for _, part := range []struct {
			name   string
			ranges loadgen.Ranges
		}{{"missing", p.Missing}, {"duplicated", p.Duplicated}, {"unexpected", p.Unexpected}, {"mismatched", p.Mismatched}} {
			if len(part.ranges) > 0 {
				fmt.Fprintf(w, " %s=%s", part.name, truncate(part.ranges.String(), 120))
			}
		}
		if p.UnexpectedIdx > 0 {
			fmt.Fprintf(w, " unexpected_idx=%d", p.UnexpectedIdx)
		}
		fmt.Fprintln(w)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
