package loadgen

import (
	"fmt"
	"strconv"
	"strings"
)

const filler = "abcdefghijklmnopqrstuvwxyz0123456789"

// ProducerID returns the ID of the i-th producer (0-based) of a run.
func ProducerID(prefix string, i int) string {
	return fmt.Sprintf("%s-%04d", prefix, i+1)
}

// Payload returns the deterministic payload of an event, so reconciliation can verify
// stored content without the generator keeping every event it sent.
// It is at least size bytes long; the padding depends on seq so a mixed-up row is detected.
func Payload(producerID, epoch string, seq uint64, size int) string {
	var b strings.Builder
	b.Grow(max(size, len(producerID)+len(epoch)+24))
	b.WriteString(producerID)
	b.WriteByte('/')
	b.WriteString(epoch)
	b.WriteByte('/')
	b.WriteString(strconv.FormatUint(seq, 10))
	if b.Len() < size {
		b.WriteByte('/')
	}
	offset := int(seq % uint64(len(filler)))
	for i := 0; b.Len() < size; i++ {
		b.WriteByte(filler[(offset+i)%len(filler)])
	}
	return b.String()
}
