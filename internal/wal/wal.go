package wal

import (
	"FairGate/internal/wire"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"sync"
)

// Set max size of a record to 1MiB
const maxRecordSize = 1 << 20

type WAL struct {
	mu   sync.Mutex
	file *os.File
}

func Open(path string) (*WAL, error) {
	//O_CREATE - Create the file if it does not exist
	//O_APPEND - Write new data at the end of the file
	//O_WRONLY - Open the file for writing only
	//0600 - Give the file owner read / write permissions
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return &WAL{file: file}, nil
}

func (w *WAL) Append(event wire.Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	//convert event into JSON
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	if len(payload) > maxRecordSize {
		return errors.New("payload too large")
	}

	var header [8]byte
	//This stores the payload length in the first four bytes of the header.
	binary.BigEndian.PutUint32(header[:4], uint32(len(payload)))
	//This calculates a CRC32 checksum of the payload and stores it in the last four bytes of the header.
	//CRC Checksum is needed for data recovery in the event of a WAL failure / corruption
	binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))

	//header[:] converts the fixed-size array into a byte slice, which can be passed to writeFull().
	//The writeFull() function ensures that all bytes are written, even if a single call to file.Write() writes only part of the data.
	if err := writeFull(w.file, header[:]); err != nil {
		return err
	}

	if err := writeFull(w.file, payload); err != nil {
		return err
	}
	//Sync() asks the operating system to flush the file's buffered changes to persistent storage.
	return w.file.Sync()
}

func (w *WAL) Close() error {
	//this function safely closes the WAL file.

	w.mu.Lock()
	defer w.mu.Unlock()

	//If w.file is already nil, it returns without doing anything.
	if w.file == nil {
		return nil
	}
	//Otherwise, it closes the file, sets the pointer to nil, and returns any error.
	err := w.file.Close()
	w.file = nil
	return err
}

func writeFull(file *os.File, data []byte) error {
	//loop continues as long as there are bytes left to write.
	for len(data) > 0 {
		//file.Write() attempts to write the bytes to the file.
		n, err := file.Write(data)
		if err != nil {
			return err
		}
		//when zero bytes were written and no error occurred, return an error.
		if n == 0 {
			return errors.New("short write to WAL")
		}

		//Removing the bytes already written
		data = data[n:]
	}
	return nil
}
