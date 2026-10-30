package wal

import (
	"FairGate/internal/wire"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// CorruptionPolicy says what recovery does with damage it cannot prove is safe to drop.
type CorruptionPolicy string

const (
	//stop recovery with ErrCorrupt; the default.
	CorruptionRefuse CorruptionPolicy = "refuse"
	//copy the segment to quarantine/, cut it at the damage, and re-append the valid records after it.
	CorruptionQuarantine CorruptionPolicy = "quarantine"
)

// ErrCorrupt reports damage to records the shipper has not delivered yet.
var ErrCorrupt = errors.New("wal: corrupt record")

// QuarantineDir is the directory, next to the WAL, where quarantined segments are copied.
const QuarantineDir = "quarantine"

type RecoverOptions struct {
	//the shipper's committed checkpoint: every record before it is already in the store.
	Shipped Position
	//what to do with damage before Shipped cannot cover; empty means CorruptionRefuse.
	OnCorruption CorruptionPolicy
}

// DamageAction is what recovery did about one bad region.
type DamageAction string

const (
	//a torn record at the end of the newest segment: a write that was never acknowledged. Cut off.
	DamageTruncatedTail DamageAction = "truncated_tail"
	//damage to records the shipper had already delivered. Left in place and skipped.
	DamageSkippedShipped DamageAction = "skipped_shipped"
	//damage to undelivered records. The segment was copied to quarantine/ and cut at the damage.
	DamageQuarantined DamageAction = "quarantined"
)

// DamageActions lists every DamageAction.
var DamageActions = []DamageAction{DamageTruncatedTail, DamageSkippedShipped, DamageQuarantined}

// Damage is one bad region recovery found and what it did about it.
type Damage struct {
	SegmentId uint64       `json:"segment_id"`
	Path      string       `json:"path"`
	Region    BadRegion    `json:"region"`
	Action    DamageAction `json:"action"`
	//for DamageQuarantined: the copy of the original segment.
	QuarantinePath string `json:"quarantine_path,omitempty"`
}

func (d Damage) String() string {
	return fmt.Sprintf("segment %d bytes %d-%d (%s): %s", d.SegmentId, d.Region.Start, d.Region.End, d.Region.Reason, d.Action)
}

// Recovery is the result of reading the WAL at startup.
type Recovery struct {
	//every valid record, in WAL order, including Salvaged.
	Events []wire.Event
	//valid records cut out of a quarantined segment. The caller must append them to the WAL again,
	//or they are never shipped.
	Salvaged []wire.Event
	Damage   []Damage
}

// Recover reads every segment with the default options: nothing is assumed shipped, and damage
// other than a torn tail in the newest segment is refused.
func Recover(path string) ([]wire.Event, error) {
	recovery, err := RecoverWithOptions(path, RecoverOptions{})
	return recovery.Events, err
}

// RecoverWithOptions reads every segment, repairs what it safely can, and returns the valid records.
//
//   - A bad region at the end of the newest segment is a write that was never acknowledged
//     (acknowledgement follows the fsync), so it is truncated.
//   - Any other bad region that lies before opts.Shipped held records that are already in the
//     store, so it is skipped and left for reclamation.
//   - Any other bad region is refused with ErrCorrupt, or quarantined when opts.OnCorruption says so.
func RecoverWithOptions(path string, opts RecoverOptions) (Recovery, error) {
	segments, err := listSegments(path)
	if err != nil {
		return Recovery{}, err
	}
	if len(segments) == 0 {
		base, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return Recovery{}, err
		}
		if err := base.Close(); err != nil {
			return Recovery{}, err
		}
		if err := syncDir(filepath.Dir(path)); err != nil {
			return Recovery{}, err
		}
		segments = []segment{{id: 0, path: path}}
	}

	var recovery Recovery
	for i, seg := range segments {
		if err := recoverSegment(seg, i == len(segments)-1, opts, &recovery); err != nil {
			return Recovery{}, err
		}
	}
	return recovery, nil
}

func recoverSegment(seg segment, isLast bool, opts RecoverOptions, recovery *Recovery) error {
	file, err := os.OpenFile(seg.path, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}
	scan, err := scanSegment(file, info.Size())
	if err != nil {
		return fmt.Errorf("read WAL segment %d: %w", seg.id, err)
	}

	//the segment is cut at the first region that is truncated or quarantined; records after the cut
	//are salvaged only when quarantining.
	cut := int64(-1)
	quarantinePath := ""
	for _, region := range scan.bad {
		damage := Damage{SegmentId: seg.id, Path: seg.path, Region: region}
		switch {
		case cut >= 0:
			//already inside the part being cut off.
			damage.Action = recovery.Damage[len(recovery.Damage)-1].Action
			damage.QuarantinePath = quarantinePath
		case isLast && region.Torn:
			damage.Action = DamageTruncatedTail
			cut = region.Start
		case opts.Shipped.atOrAfter(seg.id, region.End):
			damage.Action = DamageSkippedShipped
		case opts.OnCorruption == CorruptionQuarantine:
			if opts.Shipped.SegmentId == seg.id && opts.Shipped.Offset > region.Start {
				return fmt.Errorf("%w: segment %d bytes %d-%d (%s) contains the shipper checkpoint at offset %d; repair it by hand",
					ErrCorrupt, seg.id, region.Start, region.End, region.Reason, opts.Shipped.Offset)
			}
			if quarantinePath, err = quarantine(seg.path); err != nil {
				return fmt.Errorf("quarantine WAL segment %d: %w", seg.id, err)
			}
			damage.Action = DamageQuarantined
			damage.QuarantinePath = quarantinePath
			cut = region.Start
		default:
			return fmt.Errorf("%w in segment %d (%s) at bytes %d-%d: %s. These records have not been shipped. "+
				"Run \"fairgate wal inspect\" to see the damage, then \"fairgate wal repair\" or wal.on_corruption: quarantine "+
				"to set it aside and start",
				ErrCorrupt, seg.id, seg.path, region.Start, region.End, region.Reason)
		}
		recovery.Damage = append(recovery.Damage, damage)
	}

	for _, record := range scan.records {
		recovery.Events = append(recovery.Events, record.event)
		if cut >= 0 && record.start >= cut {
			recovery.Salvaged = append(recovery.Salvaged, record.event)
		}
	}

	if cut >= 0 {
		if err := file.Truncate(cut); err != nil {
			return err
		}
		if err := file.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// atOrAfter reports whether p is at or past offset in segment id.
func (p Position) atOrAfter(id uint64, offset int64) bool {
	return p.SegmentId > id || (p.SegmentId == id && p.Offset >= offset)
}

// quarantine copies a segment into the quarantine directory and returns the copy's path.
func quarantine(path string) (string, error) {
	dir := filepath.Join(filepath.Dir(path), QuarantineDir)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	dst := filepath.Join(dir, filepath.Base(path)+"."+time.Now().UTC().Format("20060102T150405.000000000Z"))

	src, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer src.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(out, src); err != nil {
		_ = out.Close()
		return "", err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return "", err
	}
	if err := out.Close(); err != nil {
		return "", err
	}
	return dst, syncDir(dir)
}

// syncDir makes file creations, renames, and removals in dir durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
