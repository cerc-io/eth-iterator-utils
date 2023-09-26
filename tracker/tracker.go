// This package provides a way to track multiple concurrently running trie iterators, save their
// state to a file on failures or interruptions, and restore them at the positions where they
// stopped.
//
// Example usage:
//
//	tr := tracker.New("recovery.txt", 100)
//	// Ensure the tracker is closed and saves its state
//	defer tr.CloseAndSave()
//
//	// Iterate over the trie, from one or multiple threads
//	it := tr.Tracked(tree.NodeIterator(nil))
//	for it.Next(true) {
//		// ... do work that could fail or be interrupted
//	}
//
//	// Later, restore the iterators
//	tr := tracker.New("recovery.txt", 100)
//	defer tr.CloseAndSave()
//
//	its, err := tr.Restore(tree.NodeIterator)
//	for _, it := range its {
//		// ... resume traversal
//	}
package tracker

import (
	"encoding/csv"
	"fmt"
	"os"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie"

	iter "github.com/cerc-io/eth-iterator-utils"
)

// IteratorTracker exposes a minimal interface to register and consume iterators.
type IteratorTracker interface {
	Restore(iter.IteratorConstructor) ([]trie.NodeIterator, []trie.NodeIterator, error)
	Tracked(trie.NodeIterator) trie.NodeIterator
}

var _ IteratorTracker = &Tracker{}

// Tracker is a trie iterator tracker which saves state to and restores it from a file.
type Tracker struct {
	*TrackerImpl
}

// New creates a new tracker which saves state to a given file. bufsize sets the size of the
// channel buffers used internally to manage tracking. Note that passing a bufsize smaller than the expected
// number of concurrent iterators could lead to deadlock.
func New(file string, bufsize uint) *Tracker {
	return &Tracker{NewImpl(file, bufsize)}
}

// Restore attempts to read iterator state from the recovery file.
// Returns:
// - slice of tracked iterators
// - slice of iterators originally returned by constructor
// If the file doesn't exist, returns an empty slice with no error.
// Restored iterators are constructed in the same order they appear in the returned slice.
func (tr *Tracker) Restore(makeIterator iter.IteratorConstructor) (
	[]trie.NodeIterator, []trie.NodeIterator, error,
) {
	its, bases, err := tr.TrackerImpl.Restore(makeIterator)
	if err != nil {
		return nil, nil, err
	}

	var ret []trie.NodeIterator
	for _, it := range its {
		ret = append(ret, it)
	}
	return ret, bases, nil
}

// Tracked wraps an iterator in a tracked iterator. This should not be called when the tracker can
// potentially be closed.
func (tr *Tracker) Tracked(it trie.NodeIterator) trie.NodeIterator {
	return tr.TrackerImpl.Tracked(it)
}

func NewImpl(file string, bufsize uint) *TrackerImpl {
	return &TrackerImpl{
		recoveryFile: file,
		startChan:    make(chan *Iterator, bufsize),
		stopChan:     make(chan *Iterator, bufsize),
		started:      map[*Iterator]struct{}{},
		running:      true,
	}
}

type TrackerImpl struct {
	recoveryFile string

	startChan    chan *Iterator
	stopChan     chan *Iterator
	started      map[*Iterator]struct{}
	stopped      []*Iterator
	running      bool
	sync.RWMutex // guards closing of the tracker
}

type Iterator struct {
	trie.NodeIterator
	tracker *TrackerImpl
}

func (tr *TrackerImpl) Tracked(it trie.NodeIterator) *Iterator {
	ret := &Iterator{it, tr}
	tr.startChan <- ret
	return ret
}

// Save dumps iterator path and bounds to a text file so it can be restored later.
func (tr *TrackerImpl) Save() error {
	log.Debug("Saving recovery state", "to", tr.recoveryFile)

	// if the tracker state is empty, erase any existing recovery file
	if len(tr.started) == 0 {
		return tr.removeRecoveryFile()
	}

	var rows [][]string
	for it := range tr.started {
		_, endPath := it.Bounds()
		rows = append(rows, []string{
			fmt.Sprintf("%x", it.Path()),
			fmt.Sprintf("%x", endPath),
		})
	}

	file, err := os.Create(tr.recoveryFile)
	if err != nil {
		return err
	}
	defer file.Close()
	out := csv.NewWriter(file)

	return out.WriteAll(rows)
}

func (tr *TrackerImpl) removeRecoveryFile() error {
	err := os.Remove(tr.recoveryFile)
	if os.IsNotExist(err) {
		err = nil
	}
	return err
}

func (tr *TrackerImpl) Restore(makeIterator iter.IteratorConstructor) (
	[]*Iterator, []trie.NodeIterator, error,
) {
	file, err := os.Open(tr.recoveryFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	defer file.Close()
	log.Debug("Restoring recovery state", "from", tr.recoveryFile)

	in := csv.NewReader(file)
	in.FieldsPerRecord = 2
	rows, err := in.ReadAll()
	if err != nil {
		return nil, nil, err
	}

	var wrapped []*Iterator
	var base []trie.NodeIterator
	for _, row := range rows {
		// pick up where each recovered iterator left off
		var recoveredPath []byte
		var endPath []byte

		if len(row[0]) != 0 {
			if _, err = fmt.Sscanf(row[0], "%x", &recoveredPath); err != nil {
				return nil, nil, err
			}
		}
		if len(row[1]) != 0 {
			if _, err = fmt.Sscanf(row[1], "%x", &endPath); err != nil {
				return nil, nil, err
			}
		}

		// force the lower bound path to an even length (required by geth API/HexToKeyBytes)
		if len(recoveredPath)&1 == 1 {
			// to avoid skipped nodes, we must rewind by one index
			recoveredPath = rewindPath(recoveredPath)
		}
		it := makeIterator(iter.HexToKeyBytes(recoveredPath))
		boundIt := iter.NewPrefixBoundIterator(it, endPath)
		wrapped = append(wrapped, tr.Tracked(boundIt))
		base = append(base, it)
	}

	return wrapped, base, tr.removeRecoveryFile()
}

// CloseAndSave stops all tracked iterators and dumps their state to a file.
// This closes the tracker, so adding a new iterator afterwards will fail.
// A new Tracker must be constructed in order to restore state.
func (tr *TrackerImpl) CloseAndSave() error {
	tr.Lock()
	tr.running = false
	close(tr.stopChan)
	tr.Unlock()

	// drain any pending iterators
	close(tr.startChan)
	for start := range tr.startChan {
		tr.started[start] = struct{}{}
	}
	for stop := range tr.stopChan {
		tr.stopped = append(tr.stopped, stop)
	}
	for _, stop := range tr.stopped {
		delete(tr.started, stop)
	}

	return tr.Save()
}

// Next advances the iterator, notifying its owning tracker when it finishes.
func (it *Iterator) Next(descend bool) bool {
	ret := it.NodeIterator.Next(descend)

	if !ret {
		it.tracker.RLock()
		defer it.tracker.RUnlock()
		if it.tracker.running {
			it.tracker.stopChan <- it
		} else {
			log.Error("Tracker was closed before iterator finished")
		}
	}
	return ret
}

func (it *Iterator) Bounds() ([]byte, []byte) {
	if impl, ok := it.NodeIterator.(*iter.PrefixBoundIterator); ok {
		return impl.Bounds()
	}
	return nil, nil
}

// Rewinds to the path of the previous (pre-order) node:
// If the last byte of the path is zero, pops it (e.g. [1 0] => [1]).
// Otherwise, decrements it and pads with 0xF to 64 bytes (e.g. [1] => [0 f f f ...]).
// The passed slice is not modified.
func rewindPath(path []byte) []byte {
	if len(path) == 0 {
		return path
	}
	if path[len(path)-1] == 0 {
		return path[:len(path)-1]
	}
	path[len(path)-1]--
	padded := make([]byte, 64)
	i := copy(padded, path)
	for ; i < len(padded); i++ {
		padded[i] = 0xf
	}
	return padded
}
