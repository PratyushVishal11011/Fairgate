package shipper

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointCommitAndLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")

	store := NewCheckpointStore(path)

	expected := Checkpoint{
		Segment: 3,
		Offset:  1280,
	}

	if err := store.Commit(expected); err != nil {
		t.Fatal(err)
	}

	actual, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}

	if actual != expected {
		t.Fatalf("expected %+v, got %+v", expected, actual)
	}
}

func TestCheckpointLoadMissing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")

	store := NewCheckpointStore(path)

	checkpoint, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}

	expected := Checkpoint{}

	if checkpoint != expected {
		t.Fatalf("expected %+v, got %+v", expected, checkpoint)
	}
}

func TestCheckpointCommitReplacesAtomically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")

	store := NewCheckpointStore(path)

	first := Checkpoint{
		Segment: 1,
		Offset:  500,
	}

	second := Checkpoint{
		Segment: 2,
		Offset:  900,
	}

	if err := store.Commit(first); err != nil {
		t.Fatal(err)
	}

	if err := store.Commit(second); err != nil {
		t.Fatal(err)
	}

	actual, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}

	if actual != second {
		t.Fatalf("expected %+v, got %+v", second, actual)
	}

	tempPath := path + ".tmp"

	if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
		t.Fatalf("temporary checkpoint still exists")
	}
}

func TestCheckpointRejectsInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")

	if err := os.WriteFile(
		path,
		[]byte(`not valid json`),
		0600,
	); err != nil {
		t.Fatal(err)
	}

	store := NewCheckpointStore(path)

	_, err := store.Load()
	if err == nil {
		t.Fatal("expected invalid checkpoint error")
	}
}

func TestCheckpointRejectsNegativeOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")

	if err := os.WriteFile(
		path,
		[]byte(`{"segment":2,"offset":-1}`),
		0600,
	); err != nil {
		t.Fatal(err)
	}

	store := NewCheckpointStore(path)

	_, err := store.Load()
	if err == nil {
		t.Fatal("expected invalid offset error")
	}
}

func TestCheckpointCommitAndLoadPendingBatch(t *testing.T) {
	store := NewCheckpointStore(filepath.Join(t.TempDir(), "checkpoint.json"))

	expected := Checkpoint{
		Segment: 3,
		Offset:  1280,
		Pending: &PendingBatch{Segment: 4, Offset: 64},
	}

	if err := store.Commit(expected); err != nil {
		t.Fatal(err)
	}

	actual, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}

	if actual.Segment != expected.Segment || actual.Offset != expected.Offset ||
		actual.Pending == nil || *actual.Pending != *expected.Pending {
		t.Fatalf("expected %+v pending %+v, got %+v pending %+v",
			expected, expected.Pending, actual, actual.Pending)
	}

	// Committing the batch end clears the pending batch.
	if err := store.Commit(Checkpoint{Segment: 4, Offset: 64}); err != nil {
		t.Fatal(err)
	}

	actual, err = store.Load()
	if err != nil {
		t.Fatal(err)
	}

	if actual.Pending != nil {
		t.Fatalf("expected pending batch to be cleared, got %+v", actual.Pending)
	}
}

func TestCheckpointRejectsInvalidPendingBatch(t *testing.T) {
	tests := map[string]string{
		"negative offset":   `{"segment":2,"offset":10,"pending":{"segment":3,"offset":-1}}`,
		"before checkpoint": `{"segment":2,"offset":10,"pending":{"segment":1,"offset":50}}`,
		"at checkpoint":     `{"segment":2,"offset":10,"pending":{"segment":2,"offset":10}}`,
	}

	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "checkpoint.json")
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}

			if _, err := NewCheckpointStore(path).Load(); err == nil {
				t.Fatal("expected invalid pending batch error")
			}
		})
	}
}
