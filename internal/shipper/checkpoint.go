package shipper

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type Checkpoint struct {
	Segment uint64 `json:"segment"`
	Offset  int64  `json:"offset"`
	//Pending is the end of a batch that may have reached the store but has not been committed.
	//It is written before the insert so a restart replays exactly the same WAL range with the same token.
	Pending *PendingBatch `json:"pending,omitempty"`
}

// PendingBatch is the WAL end position of an in-flight batch.
// The batch starts at the checkpoint's Segment and Offset.
type PendingBatch struct {
	Segment uint64 `json:"segment"`
	Offset  int64  `json:"offset"`
}

type CheckpointStore struct {
	path string
}

func NewCheckpointStore(path string) *CheckpointStore {
	return &CheckpointStore{path: path}
}

func (c *CheckpointStore) Load() (Checkpoint, error) {
	data, err := os.ReadFile(c.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Checkpoint{}, nil
		}

		return Checkpoint{}, err
	}

	var checkpoint Checkpoint

	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return Checkpoint{}, fmt.Errorf(
			"failed to unmarshal checkpoint: %w",
			err,
		)
	}

	if checkpoint.Offset < 0 {
		return Checkpoint{}, fmt.Errorf(
			"checkpoint offset cannot be negative: %d",
			checkpoint.Offset,
		)
	}

	if pending := checkpoint.Pending; pending != nil {
		if pending.Offset < 0 {
			return Checkpoint{}, fmt.Errorf(
				"pending batch offset cannot be negative: %d",
				pending.Offset,
			)
		}

		if pending.Segment < checkpoint.Segment ||
			(pending.Segment == checkpoint.Segment && pending.Offset <= checkpoint.Offset) {
			return Checkpoint{}, fmt.Errorf(
				"pending batch end %d:%d is not after checkpoint %d:%d",
				pending.Segment,
				pending.Offset,
				checkpoint.Segment,
				checkpoint.Offset,
			)
		}
	}

	return checkpoint, nil
}

func (c *CheckpointStore) Commit(checkpoint Checkpoint) error {
	data, err := json.Marshal(&checkpoint)
	if err != nil {
		return fmt.Errorf(
			"failed to marshal checkpoint: %w",
			err,
		)
	}

	dir := filepath.Dir(c.path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf(
			"create checkpoint directory: %w",
			err,
		)
	}

	tempPath := c.path + ".tmp"

	file, err := os.OpenFile(
		tempPath,
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		0600,
	)
	if err != nil {
		return fmt.Errorf(
			"open temporary checkpoint: %w",
			err,
		)
	}

	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf(
			"write temporary checkpoint: %w",
			err,
		)
	}

	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf(
			"sync temporary checkpoint: %w",
			err,
		)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf(
			"close temporary checkpoint: %w",
			err,
		)
	}

	if err := os.Rename(tempPath, c.path); err != nil {
		return fmt.Errorf(
			"replace checkpoint: %w",
			err,
		)
	}

	//sync the directory so the rename itself survives a power loss.
	//otherwise the previous checkpoint (and its pending batch) could reappear after a crash.
	parent, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf(
			"open checkpoint directory: %w",
			err,
		)
	}
	defer parent.Close()

	if err := parent.Sync(); err != nil {
		return fmt.Errorf(
			"sync checkpoint directory: %w",
			err,
		)
	}

	return nil
}
