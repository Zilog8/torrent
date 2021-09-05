package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent/metainfo"
)

const (
	completedBit = byte(128 >> byte(7))
	cachedBit    = byte(128 >> byte(6))
	uploadedBit  = byte(128 >> byte(5))
	closingBit   = byte(128 >> byte(1))
)

type localCacheTorrent struct {
	info              *metainfo.Info
	cachePrefix       string
	underlyingTorrent TorrentImpl
	uploadChannel     chan *localCachePiece
	closedChannel     chan bool
	currCacheSize     *uint64
	stateArray        []byte
	mutexArray        []sync.RWMutex //Handles concurrency for each piece. RLock for reading state or reading/writing local-cached data. Lock for updating state or evicting local-cached data.
	pieceArray        []PieceImpl
}

func newLocalCacheTorrent(underlyingTorrent TorrentImpl, info *metainfo.Info, cachePrefix string) localCacheTorrent {
	numPieces := info.NumPieces()
	sArray := make([]byte, numPieces)
	mArray := make([]sync.RWMutex, numPieces)
	pArray := make([]PieceImpl, numPieces)

	for i := 0; i < numPieces; i++ {
		pArray[i] = underlyingTorrent.Piece(info.Piece(i))
		com := pArray[i].Completion()
		if com.Ok && com.Complete {
			sArray[i] |= completedBit
		}
	}

	var currCacheSize uint64
	return localCacheTorrent{
		info:              info,
		cachePrefix:       cachePrefix,
		underlyingTorrent: underlyingTorrent,
		uploadChannel:     make(chan *localCachePiece, numPieces),
		closedChannel:     make(chan bool),
		currCacheSize:     &currCacheSize,
		pieceArray:        pArray,
		stateArray:        sArray,
		mutexArray:        mArray,
	}
}

func (lct *localCacheTorrent) Close() error {
	//fmt.Println("Asked to close torrent ", lct.cachePrefix)
	for i := 0; i < len(lct.mutexArray); i++ {
		func() {
			lct.mutexArray[i].Lock()
			defer lct.mutexArray[i].Unlock()
			lct.stateArray[i] |= closingBit
		}()
	}
	close(lct.uploadChannel)
	_ = <-lct.closedChannel
	//fmt.Println("Upload channel cleared; time to empty cache for ", lct.cachePrefix)
	for i := 0; i < len(lct.stateArray); i++ {
		lct.piece(i).evict()
	}

	//Close all the underlying structures
	err1 := lct.underlyingTorrent.Close()
	if err1 != nil {
		fmt.Printf(" Error closing underlying torrent storage being cached: %s\n", err1)
	}
	return nil
}

func (lct *localCacheTorrent) CurrentCacheSize() uint64 {
	return atomic.LoadUint64(lct.currCacheSize)
}

func (lct *localCacheTorrent) Piece(p metainfo.Piece) PieceImpl {
	return lct.piece(p.Index())
}

func (lct *localCacheTorrent) piece(index int) localCachePiece {
	return localCachePiece{
		index: index,
		lct:   lct,
	}
}

func (lct *localCacheTorrent) isCachedUploaded(i int) bool {
	lct.mutexArray[i].RLock()
	defer lct.mutexArray[i].RUnlock()
	return (lct.stateArray[i]&cachedBit != 0) && (lct.stateArray[i]&uploadedBit != 0)
}

func (lct *localCacheTorrent) torrentUploader() {
	for tP := range lct.uploadChannel {
		b := make([]byte, tP.length())
		var err error

		//Read from local cache
		func() {
			lct.mutexArray[tP.index].RLock()
			defer lct.mutexArray[tP.index].RUnlock()
			_, err = tP.localReadAt(b, 0)
		}()
		if err != nil {
			fmt.Printf("Error on reading piece %d from local cache for upload, for %s, will try again later: %s\n", tP.index, lct.cachePrefix, err)
			time.Sleep(10 * time.Millisecond)
			lct.uploadChannel <- tP
			continue
		}

		//Write to underlying piece
		var off int64
		var n int64
		for len(b) != 0 {
			n1, err1 := lct.pieceArray[tP.index].WriteAt(b, off)
			b = b[n1:]
			n += int64(n1)
			off += int64(n1)
			if n1 == 0 {
				err = err1
				break
			}
		}
		if err != nil || n != tP.length() {
			fmt.Printf("Error on uploading a piece from local cache to underlying storage, for %s, will try again later: %s\n", lct.cachePrefix, err)
			lct.uploadChannel <- tP
			time.Sleep(10 * time.Millisecond)
			continue
		}

		//Book-keeping
		err = lct.pieceArray[tP.index].MarkComplete()
		if err != nil {
			fmt.Printf("Error marking complete to underlying storage, for %s: %s\n", lct.cachePrefix, err)
		}
		//fmt.Printf("Uploaded piece %d of torrent %s\n", tP.index, tP.lct.cachePrefix)
		func() {
			lct.mutexArray[tP.index].Lock()
			defer lct.mutexArray[tP.index].Unlock()
			lct.stateArray[tP.index] |= uploadedBit
		}()
	}
	lct.closedChannel <- true
}
