package internal

import (
	"testing"

	"github.com/cerc-io/eth-testing/chaindata/small2"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/ethdb"
)

var (
	FixtureNodePaths = small2.Block1_StateNodePaths
	FixtureLeafKeys  = small2.Block1_StateNodeLeafKeys
)

func OpenFixtureTrie(t *testing.T, height uint64) (state.Trie, ethdb.Database) {
	data := small2.ChainData
	kvdb, ldberr := rawdb.NewLevelDBDatabase(data.ChainData, 1024, 256, t.Name(), true)
	if ldberr != nil {
		t.Fatal(ldberr)
	}
	edb, err := rawdb.NewDatabaseWithFreezer(kvdb, data.Ancient, t.Name(), true)
	if err != nil {
		t.Fatal(err)
	}

	hash := rawdb.ReadCanonicalHash(edb, height)
	header := rawdb.ReadHeader(edb, hash, height)
	if header == nil {
		t.Fatalf("unable to read canonical header at height %d", height)
	}
	sdb := state.NewDatabase(edb)
	tree, err := sdb.OpenTrie(header.Root)
	if err != nil {
		t.Fatal(err)
	}
	return tree, edb
}
