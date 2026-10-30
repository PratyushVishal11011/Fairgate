package wal

import (
	"FairGate/internal/wire"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
)

// Record formats.
//
// A legacy (v1) record is length (4) | crc32(payload) (4) | payload.
// A v2 record is magic (4) | length (4) | crc32(length || payload) (4) | payload.
//
// The magic lets recovery find the next record after a damaged one instead of giving up on the
// rest of the segment. Every record of a segment has the same format, and a segment is v2 when it
// starts with the magic: a legacy segment starts with a record length, which is at most
// maxRecordSize and so never equals the magic.
const (
	recordMagic  uint32 = 0x46475731 // "FGW1"
	v1HeaderSize        = 8
	v2HeaderSize        = 12
)

type format int

const (
	formatLegacy format = 1
	formatV2     format = 2
)

func (f format) String() string {
	if f == formatLegacy {
		return "v1"
	}
	return "v2"
}

func (f format) headerSize() int64 {
	if f == formatLegacy {
		return v1HeaderSize
	}
	return v2HeaderSize
}

var magicBytes = binary.BigEndian.AppendUint32(nil, recordMagic)

// detectFormat reads the start of a segment. An empty segment is written, and so read, as v2.
func detectFormat(r io.ReaderAt, size int64) (format, error) {
	if size == 0 {
		return formatV2, nil
	}
	prefix := make([]byte, min(size, int64(len(magicBytes))))
	if _, err := r.ReadAt(prefix, 0); err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	//a v2 segment cut inside its first header still starts with part of the magic.
	if bytes.Equal(prefix, magicBytes[:len(prefix)]) {
		return formatV2, nil
	}
	return formatLegacy, nil
}

// errBadRecord marks a record that cannot be used: torn, corrupted, or not an event.
// Other errors from decodeRecord are I/O failures.
var errBadRecord = errors.New("bad record")

// decodeRecord reads the record at offset, which must end at or before limit.
// It returns the event and the record's size in bytes. For a bad record whose length could be
// read, the size is the declared one, so callers can tell whether it runs into limit.
func decodeRecord(r io.ReaderAt, f format, offset, limit int64) (wire.Event, int64, error) {
	headerSize := f.headerSize()
	if offset+headerSize > limit {
		return wire.Event{}, headerSize, fmt.Errorf("%w: incomplete header", errBadRecord)
	}
	header := make([]byte, headerSize)
	if _, err := r.ReadAt(header, offset); err != nil {
		return wire.Event{}, 0, err
	}

	var length, checksum uint32
	if f == formatLegacy {
		length = binary.BigEndian.Uint32(header[0:4])
		checksum = binary.BigEndian.Uint32(header[4:8])
	} else {
		if binary.BigEndian.Uint32(header[0:4]) != recordMagic {
			return wire.Event{}, 0, fmt.Errorf("%w: missing record magic", errBadRecord)
		}
		length = binary.BigEndian.Uint32(header[4:8])
		checksum = binary.BigEndian.Uint32(header[8:12])
	}
	if length == 0 || length > maxRecordSize {
		return wire.Event{}, 0, fmt.Errorf("%w: invalid length %d", errBadRecord, length)
	}
	size := headerSize + int64(length)
	if offset+size > limit {
		return wire.Event{}, size, fmt.Errorf("%w: incomplete payload", errBadRecord)
	}

	payload := make([]byte, length)
	if _, err := r.ReadAt(payload, offset+headerSize); err != nil {
		return wire.Event{}, 0, err
	}
	if recordChecksum(f, header, payload) != checksum {
		return wire.Event{}, size, fmt.Errorf("%w: checksum mismatch", errBadRecord)
	}
	var event wire.Event
	if err := json.Unmarshal(payload, &event); err != nil {
		return wire.Event{}, size, fmt.Errorf("%w: invalid event: %v", errBadRecord, err)
	}
	return event, size, nil
}

// recordChecksum covers the payload, and in v2 also the length, so a damaged length is detected
// rather than read as a different record boundary.
func recordChecksum(f format, header, payload []byte) uint32 {
	if f == formatLegacy {
		return crc32.ChecksumIEEE(payload)
	}
	checksum := crc32.ChecksumIEEE(header[4:8])
	return crc32.Update(checksum, crc32.IEEETable, payload)
}

// scannedRecord is a valid record and its byte range in a segment.
type scannedRecord struct {
	start, end int64
	event      wire.Event
}

// BadRegion is a byte range of a segment that holds no valid record.
type BadRegion struct {
	Start  int64  `json:"start"`
	End    int64  `json:"end"`
	Reason string `json:"reason"`
	//the region looks like an unfinished write: nothing valid follows it (v2), or its record runs
	//into the end of the segment (v1, where nothing after a bad record can be found).
	Torn bool `json:"torn"`
}

type segmentScan struct {
	format  format
	size    int64
	records []scannedRecord
	bad     []BadRegion
}

// scanSegment reads every record of a segment. A bad record starts a bad region that ends at the
// next valid record (v2 only) or at the end of the segment.
func scanSegment(r io.ReaderAt, size int64) (segmentScan, error) {
	f, err := detectFormat(r, size)
	if err != nil {
		return segmentScan{}, err
	}
	scan := segmentScan{format: f, size: size}
	for offset := int64(0); offset < size; {
		event, n, err := decodeRecord(r, f, offset, size)
		if err == nil {
			scan.records = append(scan.records, scannedRecord{start: offset, end: offset + n, event: event})
			offset += n
			continue
		}
		if !errors.Is(err, errBadRecord) {
			return segmentScan{}, err
		}
		reason := strings.TrimPrefix(err.Error(), errBadRecord.Error()+": ")
		next := size
		torn := n > 0 && offset+n >= size
		if f == formatV2 {
			if next, err = resync(r, offset+1, size); err != nil {
				return segmentScan{}, err
			}
			torn = next == size
		}
		scan.bad = append(scan.bad, BadRegion{Start: offset, End: next, Reason: reason, Torn: torn})
		offset = next
	}
	return scan, nil
}

// resync finds the first offset at or after from where a valid v2 record starts, or size if none does.
func resync(r io.ReaderAt, from, size int64) (int64, error) {
	const chunk = 64 << 10
	buf := make([]byte, chunk+len(magicBytes)-1)
	for base := from; base < size; base += chunk {
		n, err := r.ReadAt(buf[:min(int64(len(buf)), size-base)], base)
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		window := buf[:n]
		for i := 0; ; {
			j := bytes.Index(window[i:], magicBytes)
			if j < 0 {
				break
			}
			candidate := base + int64(i+j)
			if candidate >= base+chunk {
				break
			}
			if _, _, err := decodeRecord(r, formatV2, candidate, size); err == nil {
				return candidate, nil
			} else if !errors.Is(err, errBadRecord) {
				return 0, err
			}
			i += j + 1
		}
	}
	return size, nil
}
