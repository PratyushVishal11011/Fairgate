package reconcile

import (
	"FairGate/internal/loadgen"
	"FairGate/internal/wire"
	"context"
	"testing"
	"time"
)

const testEpoch = "epoch-1"

func storedEvent(producer string, seq uint64) wire.Event {
	return wire.Event{
		ProducerId:    producer,
		ProducerEpoch: testEpoch,
		EventTime:     time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC),
		Seq:           seq,
		EventType:     "loadgen",
		Payload:       loadgen.Payload(producer, testEpoch, seq, 32),
	}
}

func testManifest() *loadgen.Manifest {
	return &loadgen.Manifest{
		Version:      loadgen.ManifestVersion,
		Epoch:        testEpoch,
		EventType:    "loadgen",
		PayloadBytes: 32,
		Producers: []loadgen.ManifestProducer{
			{ID: "a", Accepted: loadgen.Ranges{{1, 4}}, Unacked: loadgen.Ranges{{5, 6}}},
			{ID: "b", Accepted: loadgen.Ranges{{1, 2}}, Invalid: loadgen.Ranges{{3, 3}}},
		},
	}
}

func TestRunPassesWhenEveryAcceptedEventIsStoredOnce(t *testing.T) {
	src := &Memory{}
	for seq := uint64(1); seq <= 4; seq++ {
		src.Add(storedEvent("a", seq))
	}
	//one unacked event was stored: its ACK was lost after the WAL append.
	src.Add(storedEvent("a", 5), storedEvent("b", 1), storedEvent("b", 2))
	//rows from another run or producer are ignored.
	other := storedEvent("a", 9)
	other.ProducerEpoch = "other"
	src.Add(other, storedEvent("c", 1))

	report, err := Run(context.Background(), src, testManifest(), Options{VerifyContent: true})
	if err != nil {
		t.Fatal(err)
	}
	if !report.Passed || report.StoredIdentities != 7 || report.StoredUnacked != 1 || len(report.Producers) != 0 {
		t.Fatalf("expected a clean pass, got %+v", report)
	}
}

func TestRunReportsEveryViolation(t *testing.T) {
	src := &Memory{}
	//a: seq 2 missing, seq 3 stored twice, unacked seq 6 stored twice, seq 8 never sent.
	src.Add(storedEvent("a", 1), storedEvent("a", 3), storedEvent("a", 3), storedEvent("a", 4))
	src.Add(storedEvent("a", 6), storedEvent("a", 6), storedEvent("a", 8))
	//b: seq 2 stored with other content, seq 3 was refused as invalid but stored, idx 1 never sent.
	corrupted := storedEvent("b", 2)
	corrupted.Payload = "tampered"
	withIdx := storedEvent("b", 1)
	withIdx.Idx = 1
	src.Add(storedEvent("b", 1), corrupted, storedEvent("b", 3), withIdx)

	report, err := Run(context.Background(), src, testManifest(), Options{VerifyContent: true})
	if err != nil {
		t.Fatal(err)
	}
	if report.Passed {
		t.Fatal("expected violations")
	}
	if report.Missing != 1 || report.Duplicated != 2 || report.ExtraRows != 2 || report.Unexpected != 3 || report.Mismatched != 1 {
		t.Fatalf("unexpected counts %+v", report)
	}
	if len(report.Producers) != 2 {
		t.Fatalf("expected both producers reported, got %+v", report.Producers)
	}
	a, b := report.Producers[0], report.Producers[1]
	if a.Missing.String() != "2" || a.Duplicated.String() != "3,6" || a.Unexpected.String() != "8" {
		t.Fatalf("producer a: %+v", a)
	}
	if b.Mismatched.String() != "2" || b.Unexpected.String() != "3" || b.UnexpectedIdx != 1 {
		t.Fatalf("producer b: %+v", b)
	}
}

func TestRunSkipsContentUnlessAsked(t *testing.T) {
	src := &Memory{}
	for seq := uint64(1); seq <= 4; seq++ {
		event := storedEvent("a", seq)
		event.Payload = "different"
		src.Add(event)
	}
	src.Add(storedEvent("b", 1), storedEvent("b", 2))

	report, err := Run(context.Background(), src, testManifest(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !report.Passed || report.ContentVerified {
		t.Fatalf("expected a pass without content checks, got %+v", report)
	}
}

func TestRunWaitsForEventsStillBeingShipped(t *testing.T) {
	src := &Memory{}
	src.Add(storedEvent("a", 1))
	go func() {
		time.Sleep(150 * time.Millisecond)
		src.Add(storedEvent("a", 2), storedEvent("a", 3), storedEvent("a", 4), storedEvent("b", 1), storedEvent("b", 2))
	}()

	report, err := Run(context.Background(), src, testManifest(), Options{Timeout: 5 * time.Second, PollInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if !report.Passed || report.Polls < 2 {
		t.Fatalf("expected a pass after waiting, got %+v", report)
	}
}

func TestRunReportsMissingEventsAfterTimeout(t *testing.T) {
	src := &Memory{}
	src.Add(storedEvent("a", 1))

	start := time.Now()
	report, err := Run(context.Background(), src, testManifest(), Options{Timeout: 100 * time.Millisecond, PollInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if report.Passed || report.Missing != 5 {
		t.Fatalf("expected 5 missing events, got %+v", report)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("timeout not honoured: %s", elapsed)
	}
}

func TestClickHouseRejectsUnsafeTableNames(t *testing.T) {
	if _, err := OpenClickHouse([]string{"127.0.0.1:1"}, "fairgate", "u", "p", "events; DROP TABLE x"); err == nil {
		t.Fatal("expected an invalid table name to be rejected")
	}
}
