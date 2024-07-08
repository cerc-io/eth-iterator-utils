package internal

import (
	"testing"

	"github.com/cerc-io/eth-testing/chains/premerge2"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/ethdb"
)

var (
	FixtureNodePaths = premerge2.Block1_StateNodePaths
	FixtureLeafKeys  = premerge2.Block1_StateNodeLeafKeys
)

func OpenFixtureTrie(t *testing.T, height uint64) (state.Trie, ethdb.Database) {
	data := premerge2.ChainData
	edb, err := rawdb.Open(rawdb.OpenOptions{
		Directory:         data.ChainData,
		AncientsDirectory: data.Ancient,
		Namespace:         t.Name(),
		Cache:             1024,
		Handles:           256,
		ReadOnly:          true,
	})
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
