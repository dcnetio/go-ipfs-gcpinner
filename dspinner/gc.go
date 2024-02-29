package dspinner

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/bits-and-blooms/bloom/v3"
	ipfspinner "github.com/dcnetio/go-ipfs-gcpinner"
	"github.com/dcnetio/go-ipfs-gcpinner/dsindex"
	bserv "github.com/ipfs/boxo/blockservice"
	offline "github.com/ipfs/boxo/exchange/offline"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/verifcid"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// AddCleanKeys adds keys to the clean set
// The clean set is a set of keys that will be garbage collected by SGc
func (p *pinner) AddCleanKeys(ctx context.Context, recursive bool, cids ...cid.Cid) {
	if !recursive {
		for _, c := range cids {
			p.cleanIndex.Add(ctx, c.KeyString(), "")
		}
	} else {
		bsrv := bserv.New(p.bs, offline.Exchange(p.bs))
		ds := dag.NewDAGService(bsrv)
		gcs := NewSet()
		getLinks := func(ctx context.Context, cid cid.Cid) ([]*ipld.Link, error) {
			links, err := ipld.GetLinks(ctx, ds, cid)
			if err != nil {
				return nil, err
			}
			return links, nil
		}
		for _, c := range cids {
			// Walk recursively walks the dag and adds the keys to the given set
			dag.Walk(ctx, getLinks, c, gcs.Visit, dag.Concurrent(), dag.IgnoreMissing())
		}
		for _, c := range gcs.Keys() {
			p.cleanIndex.Add(ctx, c.KeyString(), "")
		}
	}
}

// RemoveCleanKeys remove keys from the clean set
func (p *pinner) RemoveCleanKeys(ctx context.Context, recursive bool, cids ...cid.Cid) {
	if !recursive {
		for _, c := range cids {
			p.cleanIndex.Delete(ctx, c.KeyString(), "")
		}
	} else {
		bsrv := bserv.New(p.bs, offline.Exchange(p.bs))
		ds := dag.NewDAGService(bsrv)
		gcs := NewSet()
		getLinks := func(ctx context.Context, cid cid.Cid) ([]*ipld.Link, error) {
			links, err := ipld.GetLinks(ctx, ds, cid)
			if err != nil {
				return nil, err
			}
			return links, nil
		}
		for _, c := range cids {
			// Walk recursively walks the dag and adds the keys to the given set
			dag.Walk(ctx, getLinks, c, gcs.Visit, dag.Concurrent(), dag.IgnoreMissing())
		}
		for _, c := range gcs.Keys() {
			p.cleanIndex.Delete(ctx, c.KeyString(), "")
		}
	}
}

// Get the number of waiting cleankeys. In big data storage scenarios, you can use this to determine whether gc is needed.
func (p *pinner) GetCleanKeysCount(ctx context.Context) int64 {
	keyNum := int64(0)
	p.cleanIndex.ForEach(ctx, "", func(key string, value string) bool {
		keyNum++
		return true
	})
	return keyNum
}

// GC performs a mark and sweep garbage collection of the blocks in the blockstore
// first, it creates a 'marked' set and adds to it the following:
// - all recursively pinned blocks, plus all of their descendants (recursively)
// - all directly pinned blocks
// - all blocks utilized internally by the pinner
//
// The routine then iterates over every block in the blockstore and
// deletes any block that is not found in the marked set and needHold return false.
// needHold is a callback function for caller to determine whether to hold a cid,return true to hold,can be nil
// if onlyCleanFlag is true, only clean the blocks in the clean set
func (p *pinner) GC(ctx context.Context, onlyCleanFlag bool, needHold func(ctx context.Context, cid cid.Cid) bool) <-chan ipfspinner.Result {
	p.lock.Lock()
	if p.gcState { //already running
		p.lock.Unlock()
		return nil
	}
	p.gcState = true
	p.cachePin = make(map[string]ipfspinner.Mode)
	p.lock.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	p.toDelKeys = []string{}
	bsrv := bserv.New(p.bs, offline.Exchange(p.bs))
	ds := dag.NewDAGService(bsrv)
	output := make(chan ipfspinner.Result, 128)
	go func() {
		defer func() {
			p.gcState = false
			close(output)
			cancel()
			debug.FreeOSMemory()
		}()
		fmt.Print(" generating bloom filter...\n")
		bloomFilter, err := p.coloredSetUseBloomfilter(ctx, ds, output)
		if err != nil {
			fmt.Print("failed to generate bloom filter\n")
			select {
			case output <- ipfspinner.Result{Error: err}:
			case <-ctx.Done():
			}
			return
		}
		var keychan <-chan cid.Cid
		if onlyCleanFlag {
			fmt.Print("onlyCleanFlag\n")
			keychan, err = allKeysChanFromIndex(ctx, p.cleanIndex)
		} else {
			fmt.Print("allKeysChan\n")
			keychan, err = p.bs.AllKeysChan(ctx)
		}
		if err != nil {
			select {
			case output <- ipfspinner.Result{Error: err}:
			case <-ctx.Done():
			}
			return
		}
		errors := false
		var removed uint64

	loop:
		for ctx.Err() == nil { // select may not notice that we're "done".
			select {
			case k, ok := <-keychan:
				if !ok {
					break loop
				}
				if !bloomFilter.Test(k.Hash()) {
					if needHold != nil && needHold(ctx, k) {
						p.cleanIndex.Delete(ctx, k.KeyString(), "")
						continue
					}
					p.toDelKeys = append(p.toDelKeys, k.KeyString())
					select {
					case <-ctx.Done():
						break loop
					default:
					}
				} else {
					p.cleanIndex.Delete(ctx, k.KeyString(), "")
				}
			case <-ctx.Done():
				break loop
			}
		}
		p.lock.Lock()
		defer p.lock.Unlock()
		fmt.Print("coloredCachePinSet begin...\n")
		gcs, err := p.coloredCachePinSet(ctx, ds, output)
		if err != nil {
			select {
			case output <- ipfspinner.Result{Error: err}:
			case <-ctx.Done():
			}
			return
		}
		fmt.Print("coloredCachePinSet end...\n")
	loop1:
		//Traverse all p.to del keys
		//If not pinned, delete
		for _, key := range p.toDelKeys {
			p.cleanIndex.Delete(ctx, key, "")
			c, err := cid.Cast([]byte(key))
			if err != nil {
				continue
			}
			if !gcs.Has(c) {
				err := p.bs.DeleteBlock(ctx, c)
				removed++
				if err != nil {
					errors = true
					select {
					case output <- ipfspinner.Result{Error: &CannotDeleteBlockError{c, err}}:
					case <-ctx.Done():
						break loop1
					}
					// continue as error is non-fatal
					continue
				}
				select {
				case output <- ipfspinner.Result{KeyRemoved: c}:
				case <-ctx.Done():
					break loop1
				}
			}
		}
		p.toDelKeys = []string{}
		if errors {
			select {
			case output <- ipfspinner.Result{Error: ErrCannotDeleteSomeBlocks}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return output
}

// allKeysChanFromIndex returns a channel that will yield all keys in the index.
func allKeysChanFromIndex(ctx context.Context, cleanIndex dsindex.Indexer) (<-chan cid.Cid, error) {
	if cleanIndex == nil {
		return nil, errors.New("cleanIndex is nil")
	}
	keychan := make(chan cid.Cid, 128)
	go func() {
		cleanIndex.ForEach(ctx, "", func(key string, value string) bool {
			c, err := cid.Cast([]byte(key))
			if err == nil {
				keychan <- c
			}
			return true
		})
		close(keychan)
	}()
	return keychan, nil
}

// onlyCleanFlag computes the set of nodes in the graph that are pinned by the
// pins in the given pinner during gc prepare.
func (p *pinner) coloredCachePinSet(ctx context.Context, ng ipld.NodeGetter, output chan<- ipfspinner.Result) (*Set, error) {
	errors := false
	gcs := NewSet()
	getLinks := func(ctx context.Context, cid cid.Cid) ([]*ipld.Link, error) {
		links, err := ipld.GetLinks(ctx, ng, cid)
		if err != nil {
			errors = true
			select {
			case output <- ipfspinner.Result{Error: &CannotFetchLinksError{cid, err}}:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return links, nil
	}
	rkeys := []cid.Cid{}
	dkeys := []cid.Cid{}
	for key, mode := range p.cachePin {
		c, err := cid.Cast([]byte(key))
		if err != nil {
			continue
		}
		if mode == ipfspinner.Recursive {
			rkeys = append(rkeys, c)
		} else {
			dkeys = append(dkeys, c)
		}
	}
	err := descendants(ctx, getLinks, gcs, rkeys)
	if err != nil {
		errors = true
		select {
		case output <- ipfspinner.Result{Error: err}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	for _, k := range dkeys {
		gcs.Add(k)
	}
	if errors {
		return nil, ErrCannotFetchAllLinks
	}
	return gcs, nil
}

// descendants recursively finds all the descendants of the given roots and
// adds them to the given cid.Set, using the provided dag.GetLinks function
// to walk the tree.
func descendants(ctx context.Context, getLinks dag.GetLinks, set *Set, roots []cid.Cid) error {
	verifyGetLinks := func(ctx context.Context, c cid.Cid) ([]*ipld.Link, error) {
		err := verifcid.ValidateCid(verifcid.DefaultAllowlist, c)
		if err != nil {
			return nil, err
		}

		return getLinks(ctx, c)
	}

	verboseCidError := func(err error) error {
		if strings.Contains(err.Error(), verifcid.ErrBelowMinimumHashLength.Error()) ||
			strings.Contains(err.Error(), verifcid.ErrPossiblyInsecureHashFunction.Error()) {
			err = fmt.Errorf("\"%s\"\nPlease run 'ipfs pin verify'"+
				" to list insecure hashes. If you want to read them,"+
				" please downgrade your go-ipfs to 0.4.13", err)
		}
		return err
	}

	for _, c := range roots {
		// Walk recursively walks the dag and adds the keys to the given set
		err := dag.Walk(ctx, verifyGetLinks, c, set.Visit, dag.Concurrent())

		if err != nil {
			err = verboseCidError(err)
			return err
		}
	}

	return nil
}

// descendants recursively finds all the descendants of the given roots and
// adds them to the given cid.Set, using the provided dag.GetLinks function
// to walk the tree.
func descendantsWithBloomfilter(ctx context.Context, getLinks dag.GetLinks, bloomFilter *bloom.BloomFilter, roots <-chan cid.Cid) error {
	verifyGetLinks := func(ctx context.Context, c cid.Cid) ([]*ipld.Link, error) {
		err := verifcid.ValidateCid(verifcid.DefaultAllowlist, c)
		if err != nil {
			return nil, err
		}
		return getLinks(ctx, c)
	}

	verboseCidError := func(err error) error {
		if strings.Contains(err.Error(), verifcid.ErrBelowMinimumHashLength.Error()) ||
			strings.Contains(err.Error(), verifcid.ErrPossiblyInsecureHashFunction.Error()) {
			err = fmt.Errorf("\"%s\"\nPlease run 'ipfs pin verify'"+
				" to list insecure hashes. If you want to read them,"+
				" please downgrade your go-ipfs to 0.4.13", err)
		}
		return err
	}
	//In order to reduce the number of repeated traversals, when the cid test is true on the bloomfilter, further determine whether all the child nodes of the cid are true on the bloomfilter test. If they are all true, it means that the cid has been traversed.
	filterVist := func(c cid.Cid) bool {
		if bloomFilter.TestAndAdd(c.Hash()) {
			// Verify the child nodes of the cid to determine whether they have been traversed
			links, err := verifyGetLinks(ctx, c)
			if err != nil {
				return false
			}
			dealtFlag := true
			count := 0
			for _, link := range links {
				if bloomFilter.Test(link.Cid.Hash()) {
					count++
					if count > 2 { //If more than 2 child nodes have been traversed, it means that the cid has been traversed (the probability of not being traversed is very low, because the false positive rate of bloomfilter is very low)
						break
					}
					continue
				}
				dealtFlag = false
				break
			}
			if dealtFlag { //All child nodes have also been traversed, which means that the current cid must have been traversed
				return false
			}
		}
		return true //If the test on bloomfilter is false, it means that the cid has not been traversed and returns true
	}
	for c := range roots {
		// Walk recursively walks the dag and adds the keys to the bloomfilter
		err := dag.Walk(ctx, verifyGetLinks, c, filterVist, dag.Concurrency(32))

		if err != nil {
			err = verboseCidError(err)
			return err
		}
	}

	return nil
}

// ColoredSet computes the set of nodes in the graph that are pinned by the
// pins in the given pinner,and use bloom filter to conserve memory.
func (p *pinner) coloredSetUseBloomfilter(ctx context.Context, ng ipld.NodeGetter, output chan<- ipfspinner.Result) (bloomFilter *bloom.BloomFilter, err error) {
	errors := false

	bloomFilter = bloom.NewWithEstimates(p.bfOptions.ExpectedNum, p.bfOptions.Fp)
	//获取当前时间
	getLinks := func(ctx context.Context, cid cid.Cid) ([]*ipld.Link, error) {
		links, err := ipld.GetLinks(ctx, ng, cid)
		if err != nil {
			errors = true
			select {
			case output <- ipfspinner.Result{Error: &CannotFetchLinksError{cid, err}}:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return links, nil
	}
	rkeys := p.RecursiveKeys(ctx)
	err = descendantsWithBloomfilter(ctx, getLinks, bloomFilter, rkeys)
	if err != nil {
		errors = true
		select {
		case output <- ipfspinner.Result{Error: err}:
		case <-ctx.Done():
			return bloomFilter, ctx.Err()
		}
	}
	dkeys := p.DirectKeys(ctx)
	for k := range dkeys {
		bloomFilter.Add(k.Hash())
	}
	if errors {
		return bloomFilter, ErrCannotFetchAllLinks
	}
	return
}

// GetGCState returns the current state of the garbage collector, whether it is running or not,retrun true if running
func (p *pinner) GetGCState() bool {
	return p.gcState
}

// ErrCannotFetchAllLinks is returned as the last Result in the GC output
// channel when there was an error creating the marked set because of a
// problem when finding descendants.
var ErrCannotFetchAllLinks = errors.New("garbage collection aborted: could not retrieve some links")

// ErrCannotDeleteSomeBlocks is returned when removing blocks marked for
// deletion fails as the last Result in GC output channel.
var ErrCannotDeleteSomeBlocks = errors.New("garbage collection incomplete: could not delete some blocks")

// CannotFetchLinksError provides detailed information about which links
// could not be fetched and can appear as a Result in the GC output channel.
type CannotFetchLinksError struct {
	Key cid.Cid
	Err error
}

// Error implements the error interface for this type with a useful
// message.
func (e *CannotFetchLinksError) Error() string {
	return fmt.Sprintf("could not retrieve links for %s: %s", e.Key, e.Err)
}

// CannotDeleteBlockError provides detailed information about which
// blocks could not be deleted and can appear as a Result in the GC output
// channel.
type CannotDeleteBlockError struct {
	Key cid.Cid
	Err error
}

// Error implements the error interface for this type with a
// useful message.
func (e *CannotDeleteBlockError) Error() string {
	return fmt.Sprintf("could not remove %s: %s", e.Key, e.Err)
}

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
