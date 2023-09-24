package service

import (
	"faydfs/config"
	"sync"
	"time"
)

var (
	//Default 1min current time - lastUpdate > softLimit allows other clients to seize the filepath held by the client (to prevent user death)
	softLimit int = config.GetConfig().Lease.LeaseSoftLimit
	//Default 1hour current time - lastUpdate > hardLimit allows LeaseManger to force the lease to be recycled and destroyed, taking into account file closing exceptions
	hardLimit int = config.GetConfig().Lease.LeaseSoftLimit
)

type lease struct {
	holder     string
	lastUpdate int64
	paths      *[]string
}

type LeaseManager struct {
	fileToMetaMap map[string]lease
	mu            sync.Mutex
}

// GetNewLeaseManager
func GetNewLeaseManager() *LeaseManager {
	fileToMetaMap := make(map[string]lease)
	lm := LeaseManager{
		fileToMetaMap: fileToMetaMap,
	}
	go lm.monitor()
	return &lm
}

func (lm *LeaseManager) monitor() {
	for {
		delay := 5 * time.Minute
		time.Sleep(delay)
		lm.mu.Lock()
		for file, fileMeta := range lm.fileToMetaMap {
			if time.Since(time.Unix(fileMeta.lastUpdate, 0)) > (time.Duration(hardLimit) * time.Millisecond) {
				lm.Revoke(fileMeta.holder, file)
			}
		}
		lm.mu.Unlock()
	}
}

func (lm *LeaseManager) Grant(client string, file string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	_, present := lm.fileToMetaMap[file]
	if present {
		return false
	}
	meta := lease{
		holder:     client,
		lastUpdate: time.Now().Unix(),
	}
	lm.fileToMetaMap[file] = meta
	return true
}

func (lm *LeaseManager) HasLock(file string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lease, present := lm.fileToMetaMap[file]
	if present {
		if time.Since(time.Unix(lease.lastUpdate, 0)) > (time.Duration(softLimit) * time.Millisecond) {
			lm.Revoke(lease.holder, file)
			return false
		}
		return true
	}
	return false
}
func (lm *LeaseManager) Revoke(client string, file string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	delete(lm.fileToMetaMap, file)
}

func (lm *LeaseManager) Renew(client string, file string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	fileMeta, present := lm.fileToMetaMap[file]
	if present {
		if fileMeta.holder == client {
			meta := lease{holder: client, lastUpdate: time.Now().Unix()}
			lm.fileToMetaMap[file] = meta
			return true
		}
		return lm.Grant(client, file)
	}
	return false
}
