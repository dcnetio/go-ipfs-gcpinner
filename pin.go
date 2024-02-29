// Package pin implements structures and methods to keep track of
// which objects a user wants to keep stored locally.
package pin

import (
	"context"
	"fmt"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

const (
	linkRecursive = "recursive"
	linkDirect    = "direct"
	linkIndirect  = "indirect"
	linkInternal  = "internal"
	linkNotPinned = "not pinned"
	linkAny       = "any"
	linkAll       = "all"
)

// Mode allows to specify different types of pin (recursive, direct etc.).
// See the Pin Modes constants for a full list.
type Mode int

// Pin Modes
const (
	// Recursive pins pin the target cids along with any reachable children.
	Recursive Mode = iota

	// Direct pins pin just the target cid.
	Direct

	// Indirect pins are cids who have some ancestor pinned recursively.
	Indirect

	// Internal pins are cids used to keep the internal state of the pinner.
	Internal

	// NotPinned
	NotPinned

	// Any refers to any pinned cid
	Any
)

// ModeToString returns a human-readable name for the Mode.
func ModeToString(mode Mode) (string, bool) {
	m := map[Mode]string{
		Recursive: linkRecursive,
		Direct:    linkDirect,
		Indirect:  linkIndirect,
		Internal:  linkInternal,
		NotPinned: linkNotPinned,
		Any:       linkAny,
	}
	s, ok := m[mode]
	return s, ok
}

// StringToMode parses the result of ModeToString() back to a Mode.
// It returns a boolean which is set to false if the mode is unknown.
func StringToMode(s string) (Mode, bool) {
	m := map[string]Mode{
		linkRecursive: Recursive,
		linkDirect:    Direct,
		linkIndirect:  Indirect,
		linkInternal:  Internal,
		linkNotPinned: NotPinned,
		linkAny:       Any,
		linkAll:       Any, // "all" and "any" means the same thing
	}
	mode, ok := m[s]

	return mode, ok
}

// ErrNotPinned is returned when trying to unpin items that are not pinned.
var ErrNotPinned = fmt.Errorf("not pinned or pinned indirectly")

// A Pinner provides the necessary methods to keep track of Nodes which are
// to be kept locally, according to a pin mode. In practice, a Pinner is in
// in charge of keeping the list of items from the local storage that should
// not be garbage-collected.
type Pinner interface {
	// IsPinned returns whether or not the given cid is pinned
	// and an explanation of why its pinned
	IsPinned(ctx context.Context, c cid.Cid) (string, bool, error)

	// IsPinnedWithType returns whether or not the given cid is pinned with the
	// given pin type, as well as returning the type of pin its pinned with.
	IsPinnedWithType(ctx context.Context, c cid.Cid, mode Mode) (string, bool, error)

	// Pin the given node, optionally recursively.
	// If recursive is true, pin all the given node's children as well.
	// If recursive is false, only pin the given node.
	// If fetchFlag is true, fetch the given node in pin deal.
	// If fetchFlag is false, only pin the given node,the caller work for fetching data.
	Pin(ctx context.Context, node ipld.Node, recursive bool, fechFlag bool) error

	// Unpin the given cid. If recursive is true, removes either a recursive or
	// a direct pin. If recursive is false, only removes a direct pin.
	// If the pin doesn't exist, return ErrNotPinned
	Unpin(ctx context.Context, cid cid.Cid, recursive bool) error

	// Update updates a recursive pin from one cid to another
	// this is more efficient than simply pinning the new one and unpinning the
	// old one
	Update(ctx context.Context, from, to cid.Cid, unpin bool) error

	// Check if a set of keys are pinned, more efficient than
	// calling IsPinned for each key
	CheckIfPinned(ctx context.Context, cids ...cid.Cid) ([]Pinned, error)

	// PinWithMode is for manually editing the pin structure. Use with
	// care! If used improperly, garbage collection may not be
	// successful.
	PinWithMode(cid.Cid, Mode)

	// RemovePinWithMode is for manually editing the pin structure.
	// Use with care! If used improperly, garbage collection may not
	// be successful.
	RemovePinWithMode(cid.Cid, Mode)

	// Flush writes the pin state to the backing datastore
	Flush(ctx context.Context) error

	// DirectKeys returns all directly pinned cids
	DirectKeys(ctx context.Context) <-chan cid.Cid

	// DirectKeys returns all recursively pinned cids
	RecursiveKeys(ctx context.Context) <-chan cid.Cid

	// GC performs a mark and sweep garbage collection of the blocks in the blockstore
	// first, it creates a 'marked' set and adds to it the following:
	// - all recursively pinned blocks, plus all of their descendants (recursively)
	// - all directly pinned blocks
	// - all blocks utilized internally by the pinner
	//
	// The routine then iterates over every block in the blockstore and
	// deletes any block that is not found in the marked set and needHold return false.
	// onlyCleanFlag is a flag to determine whether to only clean the clean set,if true,only clean the clean set
	// needHold is a callback function for caller to determine whether to hold a cid,return true to hold,can be nil
	GC(ctx context.Context, onlyCleanFlag bool, needHold func(ctx context.Context, cid cid.Cid) bool) <-chan Result

	// AddCleanKeys adds keys to the clean set
	// The clean set is a set of keys that will be garbage collected when GC is called with onlyCleanFlag set to true
	// recursive is a flag to determine whether to add the keys recursively
	AddCleanKeys(ctx context.Context, recursive bool, cids ...cid.Cid)
	// RemoveCleanKeys remove keys to the clean set
	RemoveCleanKeys(ctx context.Context, recursive bool, cids ...cid.Cid)
	// GetCleanKeysCount returns the count of clean set
	GetCleanKeysCount(ctx context.Context) int64
	// GetGCState returns the current state of the garbage collector, whether it is running or not,retrun true if running
	GetGCState() bool
}

// BloomFilterOptions is a struct that contains the options for a BloomFilter
// used by the GC.Fp is the false positive rate, and KeyNum is the number of keys
type BloomFilterOptions struct {
	Fp          float64
	ExpectedNum uint
}

// Result represents an incremental output from a garbage collection
// run.  It contains either an error, or the cid of a removed object.
type Result struct {
	KeyRemoved cid.Cid
	Error      error
}

// Pinned represents CID which has been pinned with a pinning strategy.
// The Via field allows to identify the pinning parent of this CID, in the
// case that the item is not pinned directly (but rather pinned recursively
// by some ascendant).
type Pinned struct {
	Key  cid.Cid
	Mode Mode
	Via  cid.Cid
}

// Pinned returns whether or not the given cid is pinned
func (p Pinned) Pinned() bool {
	return p.Mode != NotPinned
}

// String Returns pin status as string
func (p Pinned) String() string {
	switch p.Mode {
	case NotPinned:
		return "not pinned"
	case Indirect:
		return fmt.Sprintf("pinned via %s", p.Via)
	default:
		modeStr, _ := ModeToString(p.Mode)
		return fmt.Sprintf("pinned: %s", modeStr)
	}
}
