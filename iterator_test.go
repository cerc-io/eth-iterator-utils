package iterator_test

import (
	"bytes"
	"fmt"
	"testing"

	iter "github.com/cerc-io/eth-iterator-utils"
	"github.com/cerc-io/eth-iterator-utils/internal"
)

func TestMakePaths(t *testing.T) {
	var prefix []byte
	for i := 0; i < 4; i++ {
		nbins := uint(1) << i
		paths := iter.MakePaths(prefix, nbins)
		if len(paths) != int(nbins) {
			t.Errorf("wrong number of paths; expected %d, have %d", nbins, len(paths))
		}
	}
}

func TestIterator(t *testing.T) {
	tree, edb := internal.OpenFixtureTrie(t, 1)
	t.Cleanup(func() { edb.Close() })

	t.Run("in bounds", func(t *testing.T) {
		type testCase struct {
			lower, upper []byte
		}
		cases := []testCase{
			{nil, []byte{0, 0}},
			{[]byte{1, 0}, []byte{2, 0}},
			{[]byte{3, 5}, []byte{4, 2, 0}},
			{[]byte{5, 6, 9, 0}, []byte{7, 0}},
			{[]byte{8, 0}, []byte{8, 0}},
			{[]byte{8, 0}, []byte{7, 0}},
		}

		runCase := func(t *testing.T, tc testCase) {
			nit, err := tree.NodeIterator(iter.HexToKeyBytes(tc.lower))
			if err != nil {
				t.Fatalf("failed to create iterator: %v", err)
			}
			it := iter.NewPrefixBoundIterator(nit, tc.upper)
			for it.Next(true) {
				if bytes.Compare(it.Path(), tc.lower) < 0 {
					t.Fatalf("iterator outside lower bound: %v", it.Path())
				}
				if bytes.Compare(tc.upper, it.Path()) < 0 {
					t.Fatalf("iterator outside upper bound: %v <= %v", tc.upper, it.Path())
				}
			}
		}
		for _, tc := range cases {
			t.Run("case", func(t *testing.T) { runCase(t, tc) })
		}
	})

	t.Run("trie is covered", func(t *testing.T) {
		allPaths := internal.FixtureNodePaths
		cases := []uint{1, 2, 4, 8, 16, 32}
		runCase := func(t *testing.T, nbins uint) {
			iters, err := iter.SubtrieIterators(tree.NodeIterator, nbins)
			if err != nil {
				t.Fatalf("failed to create subtrie iterators: %v", err)
			}
			ix := 0
			for b := uint(0); b < nbins; b++ {
				for it := iters[b]; it.Next(true); ix++ {
					if !bytes.Equal(allPaths[ix], it.Path()) {
						t.Fatalf("wrong path value in bin %d (index %d)\nexpected:\t%v\nactual:\t\t%v",
							b, ix, allPaths[ix], it.Path())
					}
				}
				// if the last node path for the previous bin was even-length, the next iterator
				// will seek to the same node and it will be duplicated (see comment in Next()).
				if len(allPaths[ix-1])&1 == 0 {
					ix--
				}
			}
		}
		for _, tc := range cases {
			t.Run(fmt.Sprintf("%d bins", tc), func(t *testing.T) { runCase(t, tc) })
		}
	})
}
