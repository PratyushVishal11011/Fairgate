package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	HeaderSize           = 6
	MaxFrameSize         = 5 * (1 << 20) //Set Max Frame Size to 5 MiB
	FrameTypeEvent uint8 = 1
	FrameTypeAck   uint8 = 2
	//the length prefix counts the type and flags bytes, so no valid frame is shorter than this.
	MinFrameLength = 2
	//frame bodies are read in chunks that start at this size and double, so memory follows the bytes
	//that actually arrive rather than the length a peer claims.
	initialBodyBuffer = 64 << 10
)

var (
	//ErrMalformedFrame means the length prefix is shorter than the type and flags bytes.
	//The stream can no longer be split into frames.
	ErrMalformedFrame = errors.New("frame length is shorter than its type and flags")
)

// FrameTooLargeError reports a length prefix over the reader's limit. The body has not been read,
// so a caller that wants to keep the connection must skip Length bytes.
type FrameTooLargeError struct {
	Length uint32
	Limit  int
}

func (e *FrameTooLargeError) Error() string {
	return fmt.Sprintf("frame length %d exceeds the limit of %d bytes", e.Length, e.Limit)
}

type Frame struct {
	Type    uint8
	Flags   uint8
	Payload []byte
}

func WriteFrame(w io.Writer, f Frame) error {
	//Find total length
	length := 2 + len(f.Payload)

	if length > MaxFrameSize {
		return errors.New("Frame exceeds max length")
	}

	//Allocate the byte buffer here
	//4 = length cus 4 bytes reserved for length prefix
	//The buffer now contains bytes all set to 0
	buf := make([]byte, 4+length)
	//Encode the frame length in Big Endian with MSB coming first
	//buf[:4] puts the data in the first 4 bytes of the buffer
	//uint32 converts int to 32-bit unsigned integer
	binary.BigEndian.PutUint32(buf[:4], uint32(length))
	//Store the type and flags in the 4th and 5th byte respectively
	buf[4] = f.Type
	buf[5] = f.Flags
	//Copy the payload starting at index 6
	copy(buf[6:], f.Payload)

	//Write the buffer (as long as there are bytes to write)
	for len(buf) > 0 {
		//Important cus io.writer is a tad bit goofy and does not guarantee every byte
		//will be written in a single call
		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		//If no bytes are written without error, return an error to prevent the loop from running forever without any progress
		if n == 0 {
			return io.ErrShortWrite
		}
		//Removes bytes that have been successfully written from the buffer
		buf = buf[n:]
	}
	//Return Success
	return nil
}

func ReadFrame(r io.Reader) (Frame, error) {
	return ReadFrameLimit(r, MaxFrameSize)
}

// ReadFrameLimit reads one frame whose length prefix is at most limit.
// It returns ErrMalformedFrame or a *FrameTooLargeError without reading the body.
func ReadFrameLimit(r io.Reader, limit int) (Frame, error) {
	length, err := ReadFrameLength(r)
	if err != nil {
		return Frame{}, err
	}
	if err := CheckFrameLength(length, limit); err != nil {
		return Frame{}, err
	}
	return ReadFrameBody(r, length)
}

// ReadFrameLength reads the 4-byte big-endian length prefix of the next frame.
// It returns io.EOF only when the stream ends cleanly before the prefix.
func ReadFrameLength(r io.Reader) (uint32, error) {
	//Declare a fixed size array of 4 bytes
	var lengthBuf [4]byte

	//Read the length prefix of the buffer
	//lengthBuf[:] converts the array into a slice containing all 4 bytes
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		return 0, err
	}
	//Interprets the 4 bytes as an unsigned 32-bit integer in big endian
	return binary.BigEndian.Uint32(lengthBuf[:]), nil
}

// CheckFrameLength validates a length prefix against limit.
func CheckFrameLength(length uint32, limit int) error {
	if length < MinFrameLength {
		return ErrMalformedFrame
	}
	if uint64(length) > uint64(limit) {
		return &FrameTooLargeError{Length: length, Limit: limit}
	}
	return nil
}

// ReadFrameBody reads the type, flags, and payload of a frame whose length prefix was length.
// The buffer grows as data arrives, so a peer that claims a large frame and sends little of it
// costs little memory. A stream that ends inside the body returns io.ErrUnexpectedEOF.
func ReadFrameBody(r io.Reader, length uint32) (Frame, error) {
	if length < MinFrameLength {
		return Frame{}, ErrMalformedFrame
	}
	total := int(length)
	buf := make([]byte, min(total, initialBodyBuffer))
	if _, err := io.ReadFull(r, buf); err != nil {
		return Frame{}, unexpectedEOF(err)
	}
	for len(buf) < total {
		grown := make([]byte, min(total, 2*len(buf)))
		copy(grown, buf)
		if _, err := io.ReadFull(r, grown[len(buf):]); err != nil {
			return Frame{}, unexpectedEOF(err)
		}
		buf = grown
	}

	//Return the frame
	return Frame{
		Type:    buf[0],
		Flags:   buf[1],
		Payload: buf[2:],
	}, nil
}

// unexpectedEOF reports a clean EOF inside a frame as truncation.
func unexpectedEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}
