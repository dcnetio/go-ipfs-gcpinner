package dspinner

import (
	"context"
	"os"
	"testing"

	bs "github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	offline "github.com/ipfs/boxo/exchange/offline"
	mdag "github.com/ipfs/boxo/ipld/merkledag"
	ds "github.com/ipfs/go-datastore"
	lds "github.com/ipfs/go-ds-leveldb"
	ipld "github.com/ipfs/go-ipld-format"
)

func makeStoreLevelDB(dir string) (ds.Datastore, blockstore.Blockstore, ipld.DAGService) {
	ldstore, err := lds.NewDatastore(dir, nil)
	if err != nil {
		panic(err)
	}
	dstore := ldstore
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	return dstore, bstore, dserv
}

func makeStoreBadger(dir string) (ds.Datastore, blockstore.Blockstore, ipld.DAGService) {
	bdstore, err := lds.NewDatastore(dir, nil)
	if err != nil {
		panic(err)
	}
	dstore := &batchWrap{bdstore}
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	return dstore, bstore, dserv
}

func benchAutoSync(b *testing.B, N int, auto bool, dstore ds.Datastore, bstore blockstore.Blockstore, dserv ipld.DAGService) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pinner, err := New(ctx, dstore, bstore, dserv, nil)
	if err != nil {
		panic(err.Error())
	}

	nodes := makeNodes(N, dserv)

	pinner.SetAutosync(auto)
	pinNodes(nodes, pinner, true)
}

func BenchmarkSyncOnceBadger(b *testing.B) {
	const dsDir = "b-once"
	dstoreB1, bstoreB1, dservB1 := makeStoreBadger(dsDir)
	defer os.RemoveAll(dsDir)
	benchAutoSync(b, b.N, false, dstoreB1, bstoreB1, dservB1)
	dstoreB1.Close()
}

func BenchmarkSyncEveryBadger(b *testing.B) {
	const dsDir = "b-every"
	dstoreB2, bstoreB2, dservB2 := makeStoreBadger(dsDir)
	defer os.RemoveAll(dsDir)
	benchAutoSync(b, b.N, true, dstoreB2, bstoreB2, dservB2)
	dstoreB2.Close()
}

func BenchmarkSyncOnceLevelDB(b *testing.B) {
	const dsDir = "l-once"
	dstoreL1, bstoreL1, dservL1 := makeStoreLevelDB(dsDir)
	defer os.RemoveAll(dsDir)
	benchAutoSync(b, b.N, false, dstoreL1, bstoreL1, dservL1)
	dstoreL1.Close()
}

func BenchmarkSyncEveryLevelDB(b *testing.B) {
	const dsDir = "l-every"
	dstoreL2, bstoreL2, dservL2 := makeStoreLevelDB(dsDir)
	defer os.RemoveAll(dsDir)
	benchAutoSync(b, b.N, true, dstoreL2, bstoreL2, dservL2)
	dstoreL2.Close()
}
