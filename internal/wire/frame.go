package wire

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	HeaderSize           = 6
	MaxFrameSize         = 1 << 20 //Set Max Frame Size to 1 MiB
	FrameTypeEvent uint8 = 1
	FrameTypeAck   uint8 = 2
)

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
	//Declare a fixed size array of 4 bytes
	var lengthBuf [4]byte

	//Read the length prefix of the buffer
	//lengthBuf[:] converts the array into a slice containing all 4 bytes
	//_ discards the number of bytes (we don't really need it tbh - just find it to make sure that the read succeeded
	//If there's an error, Frame returns an empty frame
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		return Frame{}, err
	}
	//Interprets the 4 bytes as an unsigned 32-bit integer in big endian
	length := binary.BigEndian.Uint32(lengthBuf[:])

	//Validate the length
	if length < 2 || length > MaxFrameSize {
		return Frame{}, errors.New("Invalid Frame Length")
	}

	//Allocate the buffer
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Frame{}, err
	}

	//Return the frame
	return Frame{
		Type:    buf[0],
		Flags:   buf[1],
		Payload: buf[2:],
	}, nil
}
