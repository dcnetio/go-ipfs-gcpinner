package dspinner

import "github.com/ipfs/go-cid"

// Set is a implementation of a set of Cids, that is, a structure
// to which holds a single copy of every Cids with encoded cid.Hash() with base58  that is added to it.
// use cid.Hash() with base58 to avoid collisions with different cid versions
type Set struct {
	set map[string]cid.Cid
}

// NewSet initializes and returns a new Set.
func NewSet() *Set {
	return &Set{set: make(map[string]cid.Cid)}
}

// Add puts a Cid in the Set.
func (s *Set) Add(c cid.Cid) {
	s.set[c.Hash().B58String()] = c
}

// Has returns if the Set contains a given Cid.
func (s *Set) Has(c cid.Cid) bool {
	_, ok := s.set[c.Hash().B58String()]
	return ok
}

// Remove deletes a Cid from the Set.
func (s *Set) Remove(c cid.Cid) {
	delete(s.set, c.Hash().B58String())
}

// Len returns how many elements the Set has.
func (s *Set) Len() int {
	return len(s.set)
}

// Keys returns the Cids in the set.
func (s *Set) Keys() []cid.Cid {
	out := make([]cid.Cid, 0, len(s.set))
	for _, c := range s.set {
		out = append(out, c)
	}
	return out
}

// Visit adds a Cid to the set only if it is
// not in it already.
func (s *Set) Visit(c cid.Cid) bool {
	if !s.Has(c) {
		s.Add(c)
		return true
	}

	return false
}

// ForEach allows to run a custom function on each
// Cid in the set.
func (s *Set) ForEach(f func(c cid.Cid) error) error {
	for _, c := range s.set {
		err := f(c)
		if err != nil {
			return err
		}
	}
	return nil
}
