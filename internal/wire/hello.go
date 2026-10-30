package wire

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Protocol versions.
//
// The 6-byte frame header (length, type, flags) never changes. Everything else is negotiated per
// connection: the client's first frame is a hello listing the versions it speaks, and the server
// answers with a welcome naming the version both will use. Version 0 is the original protocol,
// spoken by clients that send events without a hello.
const (
	//oldest version this package speaks, apart from legacy version 0.
	MinProtocolVersion = 1
	//newest version this package speaks.
	ProtocolVersion = 1
	//protocol version of a connection whose first frame is an event rather than a hello.
	LegacyProtocolVersion = 0
)

const (
	//client to server: the first frame of a connection, offering a range of versions.
	FrameTypeHello uint8 = 3
	//server to client: the answer to a hello.
	FrameTypeWelcome uint8 = 4
)

// Welcome errors.
const (
	WelcomeUnsupportedVersion = "unsupported_version"
	WelcomeInvalidHello       = "invalid_hello"
	//a hello after the connection's version was already settled.
	WelcomeUnexpectedHello = "unexpected_hello"
)

// ErrUnsupportedVersion reports that client and server share no protocol version.
var ErrUnsupportedVersion = errors.New("no protocol version in common with the server")

// Hello opens a connection. Unknown fields are ignored, so later versions can add fields.
type Hello struct {
	MinVersion int `json:"min_version"`
	MaxVersion int `json:"max_version"`
	//free-form client name and version, for logs; at most MaxIdentifierLength bytes.
	Client string `json:"client,omitempty"`
}

// Welcome answers a hello. Either Version is set, or Error is, and then the server closes the connection.
type Welcome struct {
	//negotiated protocol version.
	Version int `json:"version,omitempty"`
	//free-form server name and version, for logs.
	Server string `json:"server,omitempty"`
	//largest frame the server accepts, counting the type and flags bytes.
	MaxFrameBytes int `json:"max_frame_bytes,omitempty"`
	//largest producer_id, producer_epoch, and event_type the server accepts.
	MaxIdentifierBytes int `json:"max_identifier_bytes,omitempty"`

	Error string `json:"error,omitempty"`
	//on unsupported_version: the range the server speaks, so the client can say what to upgrade.
	SupportedMinVersion int `json:"supported_min_version,omitempty"`
	SupportedMaxVersion int `json:"supported_max_version,omitempty"`
}

// DecodeHello parses and validates a hello payload.
func DecodeHello(data []byte) (Hello, error) {
	var hello Hello
	if err := json.Unmarshal(data, &hello); err != nil {
		return Hello{}, err
	}
	switch {
	case hello.MinVersion < 1:
		return Hello{}, errors.New("min_version must be at least 1")
	case hello.MaxVersion < hello.MinVersion:
		return Hello{}, errors.New("max_version must not be less than min_version")
	case len(hello.Client) > MaxIdentifierLength:
		return Hello{}, fmt.Errorf("client is longer than %d bytes", MaxIdentifierLength)
	}
	return hello, nil
}

// Negotiate returns the highest version in both [hello.MinVersion, hello.MaxVersion] and [minVersion, maxVersion].
func Negotiate(hello Hello, minVersion, maxVersion int) (int, bool) {
	version := min(hello.MaxVersion, maxVersion)
	if version < max(hello.MinVersion, minVersion) {
		return 0, false
	}
	return version, true
}

// Handshake sends a hello offering every version this package speaks and reads the server's welcome.
// It returns an error wrapping ErrUnsupportedVersion when the server shares no version.
func Handshake(rw io.ReadWriter, client string) (Welcome, error) {
	payload, err := json.Marshal(Hello{MinVersion: MinProtocolVersion, MaxVersion: ProtocolVersion, Client: client})
	if err != nil {
		return Welcome{}, err
	}
	if err := WriteFrame(rw, Frame{Type: FrameTypeHello, Payload: payload}); err != nil {
		return Welcome{}, err
	}
	frame, err := ReadFrame(rw)
	if err != nil {
		return Welcome{}, err
	}
	if frame.Type != FrameTypeWelcome {
		return Welcome{}, fmt.Errorf("expected a welcome frame, got type %d", frame.Type)
	}
	var welcome Welcome
	if err := json.Unmarshal(frame.Payload, &welcome); err != nil {
		return Welcome{}, fmt.Errorf("decode welcome: %w", err)
	}
	switch {
	case welcome.Error == WelcomeUnsupportedVersion:
		return welcome, fmt.Errorf("%w: server speaks %d-%d, client %d-%d", ErrUnsupportedVersion,
			welcome.SupportedMinVersion, welcome.SupportedMaxVersion, MinProtocolVersion, ProtocolVersion)
	case welcome.Error != "":
		return welcome, fmt.Errorf("server refused the hello: %s", welcome.Error)
	case welcome.Version < MinProtocolVersion || welcome.Version > ProtocolVersion:
		return welcome, fmt.Errorf("server chose protocol version %d, outside %d-%d", welcome.Version, MinProtocolVersion, ProtocolVersion)
	}
	return welcome, nil
}
