package wal

import (
	"fmt"
	"os"
)

// Verdicts for damage found by Inspect: what startup recovery would do with it.
const (
	VerdictTruncatedTail  = string(DamageTruncatedTail)
	VerdictSkippedShipped = string(DamageSkippedShipped)
	//startup refuses unless wal.on_corruption is quarantine; Repair quarantines it.
	VerdictUnshipped = "unshipped"
)

// SegmentReport describes one segment.
type SegmentReport struct {
	Id      uint64          `json:"id"`
	Path    string          `json:"path"`
	Format  string          `json:"format"`
	Size    int64           `json:"size"`
	Records int             `json:"records"`
	Damage  []DamageVerdict `json:"damage,omitempty"`
}

// DamageVerdict is a bad region and what startup recovery would do with it.
type DamageVerdict struct {
	BadRegion
	Verdict string `json:"verdict"`
}

// Inspect reads every segment without changing anything. shipped is the shipper's committed
// checkpoint, which decides whether damage is safe to skip.
func Inspect(path string, shipped Position) ([]SegmentReport, error) {
	segments, err := listSegments(path)
	if err != nil {
		return nil, err
	}
	reports := make([]SegmentReport, 0, len(segments))
	for i, seg := range segments {
		report, err := inspectSegment(seg, i == len(segments)-1, shipped)
		if err != nil {
			return nil, fmt.Errorf("segment %d: %w", seg.id, err)
		}
		reports = append(reports, report)
	}
	return reports, nil
}

func inspectSegment(seg segment, isLast bool, shipped Position) (SegmentReport, error) {
	file, err := os.Open(seg.path)
	if err != nil {
		return SegmentReport{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return SegmentReport{}, err
	}
	scan, err := scanSegment(file, info.Size())
	if err != nil {
		return SegmentReport{}, err
	}

	report := SegmentReport{
		Id:      seg.id,
		Path:    seg.path,
		Format:  scan.format.String(),
		Size:    scan.size,
		Records: len(scan.records),
	}
	for _, region := range scan.bad {
		verdict := VerdictUnshipped
		switch {
		case isLast && region.Torn:
			verdict = VerdictTruncatedTail
		case shipped.atOrAfter(seg.id, region.End):
			verdict = VerdictSkippedShipped
		}
		report.Damage = append(report.Damage, DamageVerdict{BadRegion: region, Verdict: verdict})
	}
	return report, nil
}

// Repair runs recovery with the quarantine policy and appends the salvaged records to the WAL again,
// so the gateway can start. The gateway must not be running.
func Repair(path string, shipped Position) (Recovery, error) {
	recovery, err := RecoverWithOptions(path, RecoverOptions{Shipped: shipped, OnCorruption: CorruptionQuarantine})
	if err != nil {
		return Recovery{}, err
	}
	if len(recovery.Salvaged) == 0 {
		return recovery, nil
	}
	w, err := Open(path)
	if err != nil {
		return Recovery{}, err
	}
	for _, event := range recovery.Salvaged {
		if err := w.Append(event); err != nil {
			_ = w.Close()
			return Recovery{}, fmt.Errorf("append salvaged record: %w", err)
		}
	}
	return recovery, w.Close()
}
