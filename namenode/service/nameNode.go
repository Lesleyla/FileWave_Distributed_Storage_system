package service

import (
	"context"
	"faydfs/config"
	"faydfs/proto"
	"faydfs/public"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"strings"
	"sync"
	"time"
)

// Block uhb
type blockMeta struct {
	BlockName string
	TimeStamp int64
	BlockID   int
}

// BlockMeta
type replicaMeta struct {
	blockName string
	fileSize  int64
	ipAddr    string
	state     replicaState
	replicaID int
}

// DatanodeMeta metadata of datanode
type DatanodeMeta struct {
	IPAddr             string
	DiskUsage          uint64
	HeartbeatTimeStamp int64
	Status             datanodeStatus
}

type FileMeta struct {
	FileName      string
	FileSize      uint64
	ChildFileList map[string]*FileMeta
	IsDir         bool

	Blocks []blockMeta
}

type datanodeStatus string
type replicaState string

// namenode constants
const (
	datanodeDown     = datanodeStatus("datanodeDown")
	datanodeUp       = datanodeStatus("datanodeUp")
	ReplicaPending   = replicaState("pending")
	ReplicaCommitted = replicaState("committed")
)

var (
	heartbeatTimeout = config.GetConfig().NameNode.HeartbeatTimeout
	blockSize        = config.GetConfig().Block.BlockSize
)

type NameNode struct {
	DB *DB
	// blockToLocation is not necessary to be in disk
	// blockToLocation can be obtained from datanode blockreport()
	blockToLocation map[string][]replicaMeta

	blockSize         int64
	replicationFactor int
	lock              sync.RWMutex
}

func (nn *NameNode) ShowLog() {
	dnList := nn.DB.GetDn()
	for _, v := range dnList {
		log.Printf("ip: %v", v.IPAddr)
		log.Printf("status: %v\n", v.Status)
	}
}

func GetNewNameNode(blockSize int64, replicationFactor int) *NameNode {
	namenode := &NameNode{
		blockToLocation:   make(map[string][]replicaMeta),
		DB:                GetDB("DB/leveldb"),
		blockSize:         blockSize,
		replicationFactor: replicationFactor,
	}
	namenode.DB.Put("/", &FileMeta{FileName: "/", IsDir: true, ChildFileList: map[string]*FileMeta{}})
	namenode.DB.AddDn(map[string]*DatanodeMeta{})
	go namenode.heartbeatMonitor()
	namenode.getBlockReport2DN()
	return namenode
}

// RegisterDataNode
func (nn *NameNode) RegisterDataNode(datanodeIPAddr string, diskUsage uint64) {
	nn.lock.Lock()
	defer nn.lock.Unlock()
	meta := DatanodeMeta{
		IPAddr:             datanodeIPAddr,
		DiskUsage:          diskUsage,
		HeartbeatTimeStamp: time.Now().Unix(),
		Status:             datanodeUp,
	}

	dnList := nn.DB.GetDn()
	dnList[datanodeIPAddr] = &meta
	if _, ok := dnList[datanodeIPAddr]; !ok {
		nn.DB.AddDn(dnList)
	} else {
		nn.DB.UpdateDn(dnList)
	}
}

// RenameFile
func (nn *NameNode) RenameFile(src, des string) error {
	if src == "/" {
		return public.ErrCanNotChangeRootDir
	}
	nn.lock.Lock()
	defer nn.lock.Unlock()
	//srcName, ok := nn.fileToBlock[src]
	srcName, ok := nn.DB.Get(src)
	if !ok {
		return public.ErrFileNotFound
	}
	if srcName.IsDir && len(srcName.ChildFileList) != 0 {
		return public.ErrOnlySupportRenameEmptyDir
	}
	nn.DB.Put(des, srcName)

	nn.DB.Delete(src)
	if src != "/" {
		index := strings.LastIndex(src, "/")
		parentPath := src[:index]
		if parentPath == "" {
			parentPath = "/"
		}
		fileMeta := nn.DB.GetValue(parentPath)
		newParent := &FileMeta{
			FileName:      fileMeta.FileName,
			FileSize:      fileMeta.FileSize,
			ChildFileList: fileMeta.ChildFileList,
			IsDir:         fileMeta.IsDir,
		}
		newParent.ChildFileList[des] = srcName
		delete(newParent.ChildFileList, src)
		nn.DB.Put(parentPath, newParent)
	}
	return nil
}

func (nn *NameNode) FileStat(path string) (*FileMeta, bool) {
	nn.lock.RLock()
	defer nn.lock.RUnlock()
	meta, ok := nn.DB.Get(path)
	if !ok {
		return nil, false
	}
	return meta, true
}

func (nn *NameNode) MakeDir(name string) (bool, error) {
	var path = name
	for {
		if path == "/" {
			break
		}
		index := strings.LastIndex(path, "/")
		path = path[:index]
		if path == "" {
			break
		}
		if _, ok := nn.DB.Get(path); !ok {
			return false, public.ErrPathNotFind
		}
	}
	nn.lock.Lock()
	defer nn.lock.Unlock()
	if _, ok := nn.DB.Get(name); ok {
		return false, public.ErrDirAlreadyExists
	}
	newDir := &FileMeta{
		IsDir:         true,
		ChildFileList: map[string]*FileMeta{},
	}
	nn.DB.Put(name, newDir)
	if name != "/" {
		index := strings.LastIndex(name, "/")
		parentPath := name[:index]
		if parentPath == "" {
			parentPath = "/"
		}
		//nn.fileList[parentPath].ChildFileList[name] = 0
		fileMeta := nn.DB.GetValue(parentPath)
		newParent := &FileMeta{
			FileName:      fileMeta.FileName,
			FileSize:      fileMeta.FileSize,
			ChildFileList: fileMeta.ChildFileList,
			IsDir:         fileMeta.IsDir,
		}
		newParent.ChildFileList[name] = newDir
		nn.DB.Put(parentPath, newParent)
	}
	return true, nil
}

func (nn *NameNode) DeletePath(name string) (bool, error) {

	if name == "/" {
		return false, public.ErrCanNotChangeRootDir
	}
	var path = name
	for {
		if path == "/" {
			break
		}
		index := strings.LastIndex(path, "/")
		path = path[:index]
		if path == "" {
			path = "/"
		}
		//if _, ok := nn.fileList[path]; !ok {
		if _, ok := nn.DB.Get(path); !ok {
			return false, public.ErrPathNotFind
		}
	}
	nn.lock.Lock()
	defer nn.lock.Unlock()
	//if meta, ok := nn.fileList[name]; !ok {
	if meta, ok := nn.DB.Get(name); !ok {
		return false, public.ErrPathNotFind
	} else if meta.IsDir {
		if len(meta.ChildFileList) > 0 {
			return false, public.ErrNotEmptyDir
		}
	}
	//delete(nn.fileToBlock, name)
	//delete(nn.fileList, name)
	nn.DB.Delete(name)
	if name != "/" {
		index := strings.LastIndex(name, "/")
		parentPath := name[:index]
		if parentPath == "" {
			parentPath = "/"
		}
		fileMeta := nn.DB.GetValue(parentPath)
		newParent := &FileMeta{
			FileName:      fileMeta.FileName,
			FileSize:      fileMeta.FileSize,
			ChildFileList: fileMeta.ChildFileList,
			IsDir:         fileMeta.IsDir,
		}
		delete(newParent.ChildFileList, name)
		nn.DB.Put(parentPath, newParent)

		go func() {

		}()

		//deleteSize, _ := nn.fileList[parentPath].ChildFileList[name]
		//delete(nn.fileList[parentPath].ChildFileList, name)
		//srcSize := nn.fileList[parentPath].FileSize

		//nn.fileList[parentPath].FileSize = srcSize - deleteSize
	}
	return true, nil
}

// GetDirMeta
func (nn *NameNode) GetDirMeta(name string) ([]*FileMeta, error) {
	resultList := []*FileMeta{}
	nn.lock.Lock()
	defer nn.lock.Unlock()

	//if dir, ok := nn.fileList[name]; ok && dir.IsDir {
	if dir, ok := nn.DB.Get(name); ok && dir.IsDir {
		for k, _ := range dir.ChildFileList {
			//fileMeta := nn.fileList[k]
			fileMeta := nn.DB.GetValue(k)
			resultList = append(resultList, fileMeta)
		}
		return resultList, nil
	} else if !ok {
		return nil, public.ErrPathNotFind
	} else {
		return nil, public.ErrNotDir
	}
}


func (nn *NameNode) heartbeatMonitor() {
	log.Println("========== heartbeatMonitor start ==========")
	for {
		heartbeatTimeoutDuration := time.Second * time.Duration(heartbeatTimeout)
		time.Sleep(heartbeatTimeoutDuration)
		dnList := nn.DB.GetDn()
		for ip, datanode := range nn.DB.GetDn() {
			if time.Since(time.Unix(datanode.HeartbeatTimeStamp, 0)) > heartbeatTimeoutDuration {
				go func(ip string) {
					//downDN := nn.dnList.GetValue(i)
					downDN := dnList[ip]
					if downDN.Status == datanodeDown {
						return
					}
					newStateDN := &DatanodeMeta{
						IPAddr:             downDN.IPAddr,
						DiskUsage:          downDN.DiskUsage,
						HeartbeatTimeStamp: downDN.HeartbeatTimeStamp,
						Status:             datanodeDown,
					}
					dnList[ip] = newStateDN
					nn.DB.UpdateDn(dnList)
					log.Println("============================================== dn :", downDN.IPAddr, " was down ==============================================")
					downBlocks, newIP, processIP, err := nn.reloadReplica(downDN.IPAddr)
					fmt.Println("after reloadReplica")
					if err != nil {
						log.Println("can not reloadReplica: ", err)
						return
					}
					fmt.Println(len(downBlocks))
					for j, downBlock := range downBlocks {
						err := datanodeReloadReplica(downBlocks[j], newIP[j], processIP[j])
						log.Println("==========block :", downBlock, " on datanode: ", downDN, " was Transferred to datanode: ", newIP[j], "===================")
						if err != nil {
							fmt.Println("================================== transfer err ============================================================")
							log.Println(err)
							return
						}
						go func(blockName, newIP string) {
							nn.lock.Lock()
							defer nn.lock.Unlock()
							nn.blockToLocation[blockName] = append(nn.blockToLocation[blockName], replicaMeta{
								blockName: blockName,
								ipAddr:    newIP,
								state:     ReplicaCommitted,
								replicaID: len(nn.blockToLocation[blockName]),
							})
						}(downBlocks[j], newIP[j])

					}
				}(ip)
			}
		}
	}
}

func (nn *NameNode) Heartbeat(datanodeIPAddr string, diskUsage uint64) {

	dnList := nn.DB.GetDn()
	for ip, _ := range dnList {
		if ip == datanodeIPAddr {
			log.Println("update dn:", ip, " ", datanodeIPAddr, "diskUsage :", diskUsage)
			newStateDN := &DatanodeMeta{
				IPAddr:             ip,
				DiskUsage:          diskUsage,
				HeartbeatTimeStamp: time.Now().Unix(),
				Status:             datanodeUp,
			}
			dnList[ip] = newStateDN
			nn.DB.UpdateDn(dnList)
			return
		}
	}

}

func (nn *NameNode) GetBlockReport(bl *proto.BlockLocation) {
	blockName := bl.BlockName
	ipAddr := bl.IpAddr
	blockSize := bl.BlockSize
	replicaID := int(bl.ReplicaID)
	var state replicaState
	if bl.GetReplicaState() == proto.BlockLocation_ReplicaPending {
		state = ReplicaPending
	} else {
		state = ReplicaCommitted
	}

	blockMetaList, ok := nn.blockToLocation[blockName]
	if !ok {
		nn.blockToLocation[blockName] = []replicaMeta{{
			blockName: blockName,
			ipAddr:    ipAddr,
			fileSize:  blockSize,
			replicaID: replicaID,
			state:     state,
		}}
		return
	}
	for i, _ := range blockMetaList {
		if blockMetaList[i].ipAddr == ipAddr {
			blockMetaList[i].fileSize = blockSize
			blockMetaList[i].replicaID = replicaID
			blockMetaList[i].state = state

			return
		}
	}
	var meta = replicaMeta{
		blockName: blockName,
		ipAddr:    ipAddr,
		fileSize:  blockSize,
		replicaID: replicaID,
		state:     state,
	}
	fmt.Println("=========================blockReport=========================")
	fmt.Println(meta)
	fmt.Println("=========================blockReport=========================")
	nn.blockToLocation[blockName] = append(nn.blockToLocation[blockName], meta)
	return
}

func (nn *NameNode) PutSuccess(path string, fileSize uint64, arr *proto.FileLocationArr) {
	var blockList []blockMeta
	nn.lock.Lock()
	defer nn.lock.Unlock()
	for i, list := range arr.FileBlocksList {
		//blockName := fmt.Sprintf("%v%v%v", path, "_", i)
		bm := blockMeta{
			BlockName: list.BlockReplicaList[i].BlockName,
			TimeStamp: time.Now().UnixNano(),
			BlockID:   i,
		}
		blockList = append(blockList, bm)
		var replicaList []replicaMeta
		for j, list2 := range list.BlockReplicaList {
			var state replicaState
			if list2.GetReplicaState() == proto.BlockLocation_ReplicaPending {
				state = ReplicaPending
			} else {
				state = ReplicaCommitted
			}
			rm := replicaMeta{
				blockName: list.BlockReplicaList[i].BlockName,
				fileSize:  list2.BlockSize,
				ipAddr:    list2.IpAddr,
				state:     state,
				replicaID: j,
			}
			replicaList = append(replicaList, rm)
		}
		nn.blockToLocation[list.BlockReplicaList[i].BlockName] = replicaList
	}
	//nn.fileToBlock[path] = blockList
	//nn.file2Block.Put(path, blockList)
	//nn.fileList[path] = &FileMeta{
	//	FileName:      path,
	//	FileSize:      fileSize,
	//	ChildFileList: nil,
	//	IsDir:         false,
	//}
	newFile := &FileMeta{
		FileName:      path,
		FileSize:      fileSize,
		ChildFileList: nil,
		IsDir:         false,
		Blocks:        blockList,
	}
	nn.DB.Put(path, newFile)
	if path != "/" {
		index := strings.LastIndex(path, "/")
		parentPath := path[:index]
		if parentPath == "" {
			parentPath = "/"
		}
		fileMeta := nn.DB.GetValue(parentPath)
		newParent := &FileMeta{
			FileName:      fileMeta.FileName,
			FileSize:      fileMeta.FileSize,
			ChildFileList: fileMeta.ChildFileList,
			IsDir:         fileMeta.IsDir,
		}
		newParent.ChildFileList[path] = newFile
		nn.DB.Put(parentPath, newParent)
	}
}

func (nn *NameNode) GetLocation(name string) (*proto.FileLocationArr, error) {

	blockReplicaLists := []*proto.BlockReplicaList{}
	//if block, ok := nn.fileToBlock[name]; !ok {
	if block, ok := nn.DB.Get(name); !ok {
		return nil, public.ErrPathNotFind
	} else {
		for _, meta := range block.Blocks {
			if replicaLocation, exit := nn.blockToLocation[meta.BlockName]; !exit {
				return nil, public.ErrReplicaNotFound
			} else {
				replicaList := []*proto.BlockLocation{}
				for _, location := range replicaLocation {
					var state proto.BlockLocation_ReplicaMetaState
					if location.state == ReplicaPending {
						state = proto.BlockLocation_ReplicaPending
					} else {
						state = proto.BlockLocation_ReplicaCommitted
					}
					replicaList = append(replicaList, &proto.BlockLocation{
						IpAddr:       location.ipAddr,
						BlockName:    location.blockName,
						BlockSize:    location.fileSize,
						ReplicaID:    int64(location.replicaID),
						ReplicaState: state,
					})
				}
				blockReplicaLists = append(blockReplicaLists, &proto.BlockReplicaList{
					BlockReplicaList: replicaList,
				})
			}
		}
	}

	var arr = proto.FileLocationArr{FileBlocksList: blockReplicaLists}
	return &arr, nil
}

func (nn *NameNode) WriteLocation(name string, num int64) (*proto.FileLocationArr, error) {
	var path = name
	timestamp := time.Now().UnixNano()
	for {
		if path == "/" || path == "" {
			break
		}
		index := strings.LastIndex(path, "/")
		path = path[:index]
		if path == "" {
			break
		}
		if _, ok := nn.DB.Get(path); !ok {
			return nil, public.ErrPathNotFind
		}
	}
	nn.lock.Lock()
	defer nn.lock.Unlock()
	if _, ok := nn.DB.Get(name); ok {
		return nil, public.ErrDirAlreadyExists
	}
	fileArr := proto.FileLocationArr{}
	blocks := []*proto.BlockReplicaList{}

	// A total of num * replicationFactor blocks are required (minimum number of slice blocks * number of replicas)
	//Each shard is randomly stored on four different available servers
	for i := 0; i < int(num); i++ {
		replicaIndex, err := nn.selectDN(nn.replicationFactor)
		if err != nil {
			return nil, err
		}
		replicaList := []*proto.BlockLocation{}
		for j, dn := range replicaIndex {
			realNameIndex := strings.LastIndex(name, "/")
			replicaList = append(replicaList, &proto.BlockLocation{
				IpAddr:       dn.IPAddr,
				BlockName:    fmt.Sprintf("%v%v%v%v%v", name[realNameIndex+1:], "_", timestamp, "_", i),
				BlockSize:    blockSize,
				ReplicaID:    int64(j),
				ReplicaState: proto.BlockLocation_ReplicaPending,
			})
		}
		blocks = append(blocks, &proto.BlockReplicaList{
			BlockReplicaList: replicaList,
		})
	}
	fileArr.FileBlocksList = blocks
	return &fileArr, nil
}

// needNum: number of copies required
// Using the variable starting point in map range as a random selection method reduces a large number of previous loop operations
func (nn *NameNode) selectDN(needNum int) ([]*DatanodeMeta, error) {
	result := []*DatanodeMeta{}

	dnList := nn.DB.GetDn()
	for _, dn := range dnList {
		if dn.DiskUsage > uint64(blockSize) && dn.Status != datanodeDown {
			result = append(result, dn)
		}
	}
	if len(result) < needNum {
		return nil, public.ErrNotEnoughStorageSpace
	}
	return result, nil
}

func (nn *NameNode) selectTransferDN(disableIP []string) (string, error) {
	fmt.Println("======================================================选择备份转移节点======================================================")
	dnList := nn.DB.GetDn()
outer:
	for ip, dn := range dnList {
		if dn.DiskUsage > uint64(blockSize) && dn.Status != datanodeDown {
			for _, disIp := range disableIP {
				if disIp == ip {
					continue outer
				}
			}
			fmt.Println("找到可用IP: ", ip)
			return ip, nil
		}
	}
	return "", public.ErrNotEnoughStorageSpace
}

// blockName and newIP
func (nn *NameNode) reloadReplica(downIp string) ([]string, []string, []string, error) {
	downBlocks := []string{}
	newIP := []string{}
	processIP := []string{}
	for _, location := range nn.blockToLocation {
		for i, meta := range location {
			if meta.ipAddr == downIp {
				downBlocks = append(downBlocks, meta.blockName)
				replicaMetas := nn.blockToLocation[meta.blockName]
				disableIP := []string{}
				fmt.Println(replicaMetas)
				for _, meta := range replicaMetas {
					disableIP = append(disableIP, meta.ipAddr)
				}
				fmt.Println("disabeIP: ", disableIP)
				dnIP, err := nn.selectTransferDN(disableIP)
				if err != nil {
					return nil, nil, nil, err
				}
				newIP = append(newIP, dnIP)
				if i != 0 {
					processIP = append(processIP, location[i-1].ipAddr)
				} else {
					processIP = append(processIP, location[i+1].ipAddr)
				}
			}
		}
	}
	fmt.Println("downBlocks:", downBlocks)
	fmt.Println("newIps:", newIP)
	fmt.Println("processIP:", processIP)
	return downBlocks, newIP, processIP, nil
}

func datanodeReloadReplica(blockName, newIP, processIP string) error {
	conn, client, _, _ := getGrpcN2DConn(processIP)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	//status, err := (*client).GetDirMeta(ctx, &proto.PathName{PathName: remoteDirPath})
	log.Println("replicate "+blockName+" to ", newIP)
	_, err := (*client).ReloadReplica(ctx, &proto.CopyReplica2DN{BlockName: blockName, NewIP: newIP})
	if err != nil {
		log.Print("datanode ReloadReplica fail: processIP :", err)
		return err
	}
	return nil
}

func (nn *NameNode) getBlockReport2DN() {
	dnList := nn.DB.GetDn()
	for ip, dn := range dnList {
		if dn.Status != datanodeDown {
			blockReplicaList, err := nn.getBlockReportRPC(ip)
			if err != nil {
				log.Println(err)
				return
			}
			for _, bm := range blockReplicaList.BlockReplicaList {
				nn.GetBlockReport(bm)
			}
		}
	}

}

func (nn *NameNode) getBlockReportRPC(addr string) (*proto.BlockReplicaList, error) {
	conn, client, _, _ := getGrpcN2DConn(addr)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	//status, err := (*client).GetDirMeta(ctx, &proto.PathName{PathName: remoteDirPath})
	blockReplicaList, err := (*client).GetBlockReport(ctx, &proto.Ping{Ping: addr})
	if err != nil {
		log.Print("datanode get BlockReport fail: addr :", addr)
		return nil, err
	}
	return blockReplicaList, nil
}

func getGrpcN2DConn(address string) (*grpc.ClientConn, *proto.N2DClient, *context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	//conn, err := grpc.DialContext(ctx, address, grpc.WithBlock())
	conn2, err := grpc.DialContext(ctx, address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect to %v error %v", address, err)
	}
	client := proto.NewN2DClient(conn2)
	return conn2, &client, &cancel, err
}
