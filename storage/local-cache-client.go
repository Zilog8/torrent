package storage

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"time"

	"github.com/anacrolix/torrent/metainfo"
)

var _ ClientImplCloser = (*localCacheClient)(nil)

type localCacheClient struct {
	localDir         string
	maxSize          uint64 //Max cached data
	currSize         uint64 //Current cached data
	underlyingClient ClientImpl
	torrentMap       map[metainfo.Hash]*localCacheTorrent
	torrentMapMutex  sync.Mutex
}

func NewLocalCache(localDir string, maxSize uint64, underlying ClientImpl) *localCacheClient {
	lcc := &localCacheClient{
		localDir:         localDir,
		maxSize:          maxSize,
		underlyingClient: underlying,
		torrentMap:       make(map[metainfo.Hash]*localCacheTorrent),
	}

	if maxSize > 0 {
		go func() {
			for {
				time.Sleep(time.Second)
				lcc.manageCache()
			}
		}()
	}
	return lcc
}

func (lcc *localCacheClient) manageCache() {
	lcc.torrentMapMutex.Lock()
	defer lcc.torrentMapMutex.Unlock()
	var totalCache uint64
	for _, t := range lcc.torrentMap {
		totalCache += t.CurrentCacheSize()
	}

	if lcc.maxSize > 0 && totalCache > lcc.maxSize {
		//fmt.Println("State of the cache is full: ", totalCache)

		var possibles []localCachePiece
		for _, t := range lcc.torrentMap {
			for i := 0; i < len(t.stateArray); i++ {
				if t.isCachedUploaded(i) {
					possibles = append(possibles, t.piece(i))
				}
			}
		}
		//fmt.Println("Number of eviction candidates: ", len(possibles))
		target := lcc.maxSize
		if target > 256000000 {
			target -= 25600000 //limit evicting for large caches; e.g. evicting 2gb from a 20gb cache would be kinda silly
		} else {
			target -= target / 10
		}

		for totalCache > target && len(possibles) > 0 {
			i := rand.Intn(len(possibles))
			totalCache -= possibles[i].evict()
			possibles[i] = possibles[len(possibles)-1]
			possibles = possibles[:len(possibles)-1]
		}
		//fmt.Println("State of the cache after evictions: ", totalCache)
		//fmt.Println("Number of eviction candidates that survived: ", len(possibles))

	} //else {
	//fmt.Printf("State of the cache is unfull %d of %d\n", totalCache, lcc.maxSize)
	//}
}

func (lcc *localCacheClient) OpenTorrent(info *metainfo.Info, infoHash metainfo.Hash) (TorrentImpl, error) {
	//fmt.Println("Opening Torrent")

	underlyingTorrent, err := lcc.underlyingClient.OpenTorrent(info, infoHash)
	if err != nil {
		fmt.Printf("Error creating underlying storage client: %s\n", err)
		return TorrentImpl{}, err
	}

	cachePrefix := filepath.Join(lcc.localDir, infoHash.HexString())

	t := newLocalCacheTorrent(underlyingTorrent, info, cachePrefix)

	func() {
		lcc.torrentMapMutex.Lock()
		defer lcc.torrentMapMutex.Unlock()
		lcc.torrentMap[infoHash] = &t
	}()

	go t.torrentUploader()

	//fmt.Println("Torrent Open")
	return TorrentImpl{Piece: t.Piece, Close: t.Close}, err
}

func (lcc *localCacheClient) Close() error {
	lcc.torrentMapMutex.Lock()
	defer lcc.torrentMapMutex.Unlock()
	//fmt.Println("Asked to close the local cache at ", lcc.localDir)
	for _, t := range lcc.torrentMap {
		t.Close()
	}
	return nil
}
