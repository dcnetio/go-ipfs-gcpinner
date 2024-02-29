package dspinner

import (
	"bytes"
	"context"
	"testing"

	ipfspinner "github.com/dcnetio/go-ipfs-gcpinner"
	blockstore "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
)

func Test_pinner_GC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore, bstore, dserv := makeStore()

	p, err := New(ctx, dstore, bstore, dserv, nil)
	if err != nil {
		t.Fatal(err)
	}
	a, ak := randNode()
	err = dserv.Add(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	gcOut := p.GC(ctx, false, nil)
	assertGc(t, bstore, gcOut, []cid.Cid{ak}, "gc not remove unpinned node")
	// Pin A{}
	err = p.Pin(ctx, a, false, true)
	if err != nil {
		t.Fatal(err)
	}
	gcOut = p.GC(ctx, false, nil)
	assertNotGc(t, gcOut, []cid.Cid{ak}, "gc  remove pinned node")
	// create new node c, to be indirectly pinned through b
	c, _ := randNode()
	err = dserv.Add(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	ck := c.Cid()

	w, wk := randNode()
	err = dserv.Add(ctx, w)
	if err != nil {
		t.Fatal(err)
	}

	// Create new node b, to be parent to a and c
	b, _ := randNode()
	err = b.AddNodeLink("child", a)
	if err != nil {
		t.Fatal(err)
	}
	err = b.AddNodeLink("otherchild", c)
	if err != nil {
		t.Fatal(err)
	}
	err = b.AddNodeLink("wchild", w)
	if err != nil {
		t.Fatal(err)
	}
	err = dserv.Add(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	bk := b.Cid()
	// recursively pin B{A,C,W}
	err = p.Pin(ctx, b, true, true)
	if err != nil {
		t.Fatal(err)
	}
	gcOut = p.GC(ctx, false, nil)
	assertNotGc(t, gcOut, []cid.Cid{ck, ak, bk, wk}, "gc remove pinned node")

	d, _ := randNode()
	err = d.AddNodeLink("a", a)
	if err != nil {
		panic(err)
	}

	e, ek := randNode()
	err = d.AddNodeLink("e", e)
	if err != nil {
		panic(err)
	}

	// Must be in dagserv for unpin to work
	err = dserv.Add(ctx, e)
	if err != nil {
		t.Fatal(err)
	}

	err = d.AddNodeLink("w", w)
	if err != nil {
		panic(err)
	}
	dk := d.Cid()
	// Add D{A,E,W}
	err = p.Pin(ctx, d, true, true)
	if err != nil {
		t.Fatal(err)
	}

	gcOut = p.GC(ctx, false, nil)
	assertNotGc(t, gcOut, []cid.Cid{dk, ak, ek, ck, bk, ek}, "gc remove pinned node")
	err = p.Unpin(ctx, bk, true)
	if err != nil {
		t.Fatal(err)
	}
	gcOut = p.GC(ctx, false, nil)
	assertGc(t, bstore, gcOut, []cid.Cid{bk, ck}, "gc not remove unpinned node")
	assertPinned(t, p, wk, "Failed to find key")
	assertPinned(t, p, ak, "Failed to find key")
	err = p.Unpin(ctx, dk, true)
	if err != nil {
		t.Fatal(err)
	}
	gcOut = p.GC(ctx, false, nil)
	assertGc(t, bstore, gcOut, []cid.Cid{dk, ek, wk}, "gc not remove unpinned node")
	assertPinned(t, p, ak, "Failed to find key")
	err = p.Unpin(ctx, ak, false)
	if err != nil {
		t.Fatal(err)
	}
	gcOut = p.GC(ctx, false, nil)
	assertGc(t, bstore, gcOut, []cid.Cid{ak}, "gc not remove unpinned node")

}

func Test_pinner_cleanset_GC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dstore, bstore, dserv := makeStore()

	p, err := New(ctx, dstore, bstore, dserv, nil)
	if err != nil {
		t.Fatal(err)
	}
	a, ak := randNode()
	err = dserv.Add(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	// Pin A{}
	err = p.Pin(ctx, a, false, true)
	if err != nil {
		t.Fatal(err)
	}
	p.AddCleanKeys(ctx, false, ak)
	gcOut := p.GC(ctx, true, nil)
	assertNotGc(t, gcOut, []cid.Cid{ak}, "gc  remove pinned node")
	// create new node c, to be indirectly pinned through b
	c, _ := randNode()
	err = dserv.Add(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	ck := c.Cid()

	w, wk := randNode()
	err = dserv.Add(ctx, w)
	if err != nil {
		t.Fatal(err)
	}

	// Create new node b, to be parent to a and c
	b, _ := randNode()
	err = b.AddNodeLink("child", a)
	if err != nil {
		t.Fatal(err)
	}
	err = b.AddNodeLink("otherchild", c)
	if err != nil {
		t.Fatal(err)
	}
	err = b.AddNodeLink("wchild", w)
	if err != nil {
		t.Fatal(err)
	}

	err = dserv.Add(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	bk := b.Cid()
	// recursively pin B{A,C,W}
	err = p.Pin(ctx, b, true, true)
	if err != nil {
		t.Fatal(err)
	}
	p.AddCleanKeys(ctx, false, bk)
	gcOut = p.GC(ctx, true, nil)
	assertNotGc(t, gcOut, []cid.Cid{ck, ak, bk, wk}, "gc remove pinned node")
	err = p.Unpin(ctx, bk, true)
	if err != nil {
		t.Fatal(err)
	}
	gcOut = p.GC(ctx, true, nil)
	assertNotGc(t, gcOut, []cid.Cid{wk}, "gc remove  node not in clean set")
	assertPinned(t, p, ak, "Failed to find key")
	assertUnpinned(t, p, ck, "Found key")
	assertUnpinned(t, p, bk, "Found key")
}

func assertGc(t *testing.T, bs blockstore.Blockstore, gcOut <-chan ipfspinner.Result, cids []cid.Cid, failmsg string) {
	cb := func(c1 cid.Cid, err error) {
		if err != nil {
			t.Fatal(err)
		}
		for i, c := range cids {
			if c1.Hash().B58String() == c.Hash().B58String() {
				flag, _ := bs.Has(context.Background(), c1)
				if flag { // if blockstore has the block, so it is not gc successfully
					continue
				}
				flag, _ = bs.Has(context.Background(), c)
				if flag { // if blockstore has the block, so it is not gc successfully
					continue
				}
				if i == len(cids)-1 {
					cids = cids[:i]
				} else {
					cids = append(cids[:i], cids[i+1:]...)
				}
			}
		}
	}
	err := collectResult(context.Background(), gcOut, cb)
	if err != nil {
		t.Fatal(err)
	}
	if len(cids) != 0 {
		t.Fatal(failmsg)
	}
}

func assertNotGc(t *testing.T, gcOut <-chan ipfspinner.Result, cids []cid.Cid, failmsg string) {
	gcFlag := false
	cb := func(c1 cid.Cid, err error) {
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cids {
			if c1.Hash().B58String() == c.Hash().B58String() {
				gcFlag = true
			}
		}

	}
	err := collectResult(context.Background(), gcOut, cb)
	if err != nil {
		t.Fatal(err)
	}
	if gcFlag {
		t.Fatal(failmsg)
	}
}

// collectResult collects the output of a garbage collection run and calls the
// given callback for each object removed.  It also collects all errors into a
// MultiError which is returned after the gc is completed.
func collectResult(ctx context.Context, gcOut <-chan ipfspinner.Result, cb func(c cid.Cid, err error)) error {
	var errors []error
loop:
	for {
		select {
		case res, ok := <-gcOut:
			if !ok {
				break loop
			}
			if res.Error != nil {
				errors = append(errors, res.Error)
			} else if res.KeyRemoved.Defined() && cb != nil {
				cb(res.KeyRemoved, nil)
			}
		case <-ctx.Done():
			errors = append(errors, ctx.Err())
			break loop
		}
	}
	switch len(errors) {
	case 0:
		return nil
	case 1:
		return errors[0]
	default:
		return newMultiError(errors...)
	}
}

// NewMultiError creates a new MultiError object from a given slice of errors.
func newMultiError(errs ...error) *multiError {
	return &multiError{errs[:len(errs)-1], errs[len(errs)-1]}
}

// MultiError contains the results of multiple errors.
type multiError struct {
	Errors  []error
	Summary error
}

func (e *multiError) Error() string {
	var buf bytes.Buffer
	for _, err := range e.Errors {
		buf.WriteString(err.Error())
		buf.WriteString("; ")
	}
	buf.WriteString(e.Summary.Error())
	return buf.String()
}
