package storage

import (
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"

	"github.com/anacrolix/missinggo/v2"
	"github.com/anacrolix/torrent/metainfo"
)

var _ SelfHashing = (*localCachePiece)(nil)
var _ PieceImpl = (*localCachePiece)(nil)

type localCachePiece struct {
	index int
	lct   *localCacheTorrent
}

func (lcp localCachePiece) Completion() (com Completion) {
	lcp.lct.mutexArray[lcp.index].RLock()
	defer lcp.lct.mutexArray[lcp.index].RUnlock()
	com.Complete = lcp.lct.stateArray[lcp.index]&completedBit != 0
	com.Ok = true
	return
}

func (lcp localCachePiece) MarkComplete() error {
	lcp.lct.mutexArray[lcp.index].Lock()
	defer lcp.lct.mutexArray[lcp.index].Unlock()
	//fmt.Printf("Marking Complete piece %d of torrent %s\n", lcp.index, lcp.lct.cachePrefix)
	lcp.lct.stateArray[lcp.index] |= completedBit
	if lcp.lct.stateArray[lcp.index]&closingBit == 0 { //make sure the world's not ending / channel's closed
		lcp.lct.uploadChannel <- &lcp
	}
	return nil
}

func (lcp localCachePiece) MarkNotComplete() (err error) {
	func() {
		lcp.lct.mutexArray[lcp.index].Lock()
		defer lcp.lct.mutexArray[lcp.index].Unlock()
		lcp.lct.stateArray[lcp.index] &= ^(completedBit | cachedBit)
	}()
	err = lcp.lct.pieceArray[lcp.index].MarkNotComplete()
	if err != nil {
		fmt.Printf("Error marking incomplete to underlying storage, for %s: %s\n", lcp.lct.cachePrefix, err)
	}
	return
}

func (lcp localCachePiece) ReadAt(b []byte, off int64) (i int, err error) {
	//if locally cached, read from cache
	func() {
		lcp.lct.mutexArray[lcp.index].RLock()
		defer lcp.lct.mutexArray[lcp.index].RUnlock()
		if lcp.lct.stateArray[lcp.index]&cachedBit != 0 {
			i, err = lcp.localReadAt(b, off)
			if err != nil {
				fmt.Printf("Error with reading local cache for %s, piece %d,   %s\n", lcp.lct.cachePrefix, lcp.index, err)
				i = 0
			}
		}
	}()
	if i == len(b) && err == nil {
		return
	}

	//Read from to the underlying storage if it has it.
	underCompletion := lcp.lct.pieceArray[lcp.index].Completion()
	if underCompletion.Ok && underCompletion.Complete {
		//fmt.Println("read from underlying!")
		//TODO: If local cache severely empty (<75%?), read the whole piece and cache it.
		i, err = lcp.lct.pieceArray[lcp.index].ReadAt(b, off)
	}
	return
}

func (lcp localCachePiece) length() int64 {
	if lcp.index < len(lcp.lct.pieceArray)-1 {
		return lcp.lct.info.PieceLength
	}
	return lcp.lct.info.TotalLength() % lcp.lct.info.PieceLength
}

func (lcp localCachePiece) localFilename() string {
	return lcp.lct.cachePrefix + "-" + strconv.Itoa(lcp.index)
}

func (lcp localCachePiece) localReadAt(b []byte, off int64) (n int, err error) {
	//fmt.Println("Asked to do a local read")
	filename := lcp.localFilename()
	f, err := os.Open(filename)
	if err != nil {
		return
	}
	defer f.Close()
	for len(b) != 0 {
		n1, err1 := f.ReadAt(b, off)
		b = b[n1:]
		n += n1
		off += int64(n1)
		if n1 == 0 {
			err = err1
			break
		}
	}
	//fmt.Println("Completed local read")
	return
}

func (lcp localCachePiece) WriteAt(b []byte, off int64) (n int, err error) {
	var alreadyComplete bool
	func() {
		lcp.lct.mutexArray[lcp.index].RLock()
		defer lcp.lct.mutexArray[lcp.index].RUnlock()
		alreadyComplete = lcp.lct.stateArray[lcp.index]&completedBit != 0
	}()
	if alreadyComplete {
		//fmt.Printf("Got a write for a supposedly complete piece of torrent %s. Will ignore write to piece %d", lcp.lct.cachePrefix, lcp.index)
		return
	}

	filename := lcp.localFilename()
	os.MkdirAll(filepath.Dir(filename), 0777)
	var f *os.File
	f, err = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf("Error with opening for write local cache for %s, piece %d,   %s\n", lcp.lct.cachePrefix, lcp.index, err)
		return
	}
	//increase cache size counter if we haven't done so yet
	func() {
		lcp.lct.mutexArray[lcp.index].Lock()
		defer lcp.lct.mutexArray[lcp.index].Unlock()
		if lcp.lct.stateArray[lcp.index]&cachedBit == 0 {
			lcp.lct.stateArray[lcp.index] |= cachedBit
			atomic.AddUint64(lcp.lct.currCacheSize, uint64(lcp.length()))
		}
	}()

	var n1 int
	n1, err = f.WriteAt(b, off)
	closeErr := f.Close()
	n += n1
	if err == nil {
		err = closeErr
	}
	if err == nil && n1 != len(b) {
		err = io.ErrShortWrite
	}

	return
}

func (lcp localCachePiece) SelfHash() (hash metainfo.Hash, err error) {
	//Do we have a local copy?
	local := false
	func() {
		lcp.lct.mutexArray[lcp.index].RLock()
		defer lcp.lct.mutexArray[lcp.index].RUnlock()
		if lcp.lct.stateArray[lcp.index]&cachedBit != 0 {
			b := make([]byte, lcp.length())
			_, err = lcp.localReadAt(b, 0)
			if err != nil {
				fmt.Printf("Error on reading piece %d from local cache for hashing, for %s: %s\n", lcp.index, lcp.lct.cachePrefix, err)
			} else {
				missinggo.CopyExact(&hash, sha1.Sum(b))
				local = true
			}
		}
	}()
	if local {
		return
	}

	//No local. Does the unerlying storage want to do its own hashing?
	if i, ok := lcp.lct.pieceArray[lcp.index].(SelfHashing); ok {
		var sum metainfo.Hash
		//fmt.Printf("A piece self-hashed: %d\n", lcp.index)
		sum, err = i.SelfHash()
		missinggo.CopyExact(&hash, sum)
		return
	}

	//No other choice but to read the underlying I guess
	b := make([]byte, lcp.length())
	var off int64
	for len(b) != 0 {
		n1, err1 := lcp.lct.pieceArray[lcp.index].ReadAt(b, off)
		b = b[n1:]
		off += int64(n1)
		if n1 == 0 {
			err = err1
			break
		}
	}

	if err != nil {
		fmt.Printf("Error on reading piece %d from underlying storage for hashing, for %s: %s\n", lcp.index, lcp.lct.cachePrefix, err)
	} else {
		missinggo.CopyExact(&hash, sha1.Sum(b))
	}
	return
}

func (lcp localCachePiece) evict() (freed uint64) {
	lcp.lct.mutexArray[lcp.index].Lock()
	defer lcp.lct.mutexArray[lcp.index].Unlock()
	//fmt.Printf("Attempting Evict for piece %d of %s\n", lcp.index, lcp.lct.cachePrefix)
	if lcp.lct.stateArray[lcp.index]&cachedBit != 0 {
		err := os.Remove(lcp.localFilename())
		if err != nil {
			fmt.Printf("Error evicting local cache piece %d of %s : %s\n", lcp.index, lcp.lct.cachePrefix, err)
		} else {
			freed = uint64(lcp.length())
			atomic.AddUint64(lcp.lct.currCacheSize, ^uint64(freed-1))
			lcp.lct.stateArray[lcp.index] &= ^cachedBit
			//fmt.Printf("Successful Evict for piece %d of %s\n", lcp.index, lcp.lct.cachePrefix)
		}
	}
	return
}
