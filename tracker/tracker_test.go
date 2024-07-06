package tracker_test

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/cerc-io/eth-iterator-utils/internal"
	"github.com/cerc-io/eth-iterator-utils/tracker"
)

func TestTracker(t *testing.T) {
	NumIters := uint(1)
	recoveryFile := filepath.Join(t.TempDir(), "tracker_test.csv")

	tree, edb := internal.OpenFixtureTrie(t, 1)
	t.Cleanup(func() { edb.Close() })

	// traverse trie and trigger error at some intermediate point
	N := len(internal.FixtureNodePaths)
	interrupt := rand.Intn(N/2) + N/4
	failedTraverse := func() []byte {
		tr := tracker.New(recoveryFile, NumIters)
		defer tr.CloseAndSave()

		count := 0
		nodeit, err := tree.NodeIterator(nil)
		if err != nil {
			t.Fatal(err)
		}
		for it := tr.Tracked(nodeit); it.Next(true); {
			if count == interrupt {
				t.Logf("interrupting at: i=%d path=%v", count, it.Path())
				return it.Path()
			}
			count++
		}
		return nil
	}

	failedAt := failedTraverse()
	if failedAt == nil {
		t.Fatal("traversal wasn't interrupted")
	}

	if !fileExists(recoveryFile) {
		t.Fatal("recovery file wasn't created")
	}

	tr := tracker.New(recoveryFile, NumIters)
	its, _, err := tr.Restore(tree.NodeIterator)
	if err != nil {
		t.Fatal(err)
	}
	if uint(len(its)) != NumIters {
		t.Fatalf("expected to restore %d iterators, got %d", NumIters, len(its))
	}
	if !its[0].Next(true) {
		t.Fatal("iterator ends prematurely after restore")
	}
	if !bytes.Equal(failedAt, its[0].Path()) {
		// Due to the constraint that NodeIterator can only be initialized with an even-length path,
		// we sometimes rewind an extra node when restoring (e.g. [1 2 0] => [1 2]).
		if !its[0].Next(true) {
			t.Fatal("iterator ends prematurely after restore")
		}
		if !bytes.Equal(failedAt, its[0].Path()) {
			t.Fatalf("iterator restored to wrong position: expected %v, got %v", failedAt, its[0].Path())
		}
	}

	if fileExists(recoveryFile) {
		t.Fatal("recovery file wasn't removed")
	}
}

func fileExists(file string) bool {
	_, err := os.Stat(file)
	return !os.IsNotExist(err)
}
