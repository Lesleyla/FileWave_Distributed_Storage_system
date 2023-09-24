package main

import (
	"context"
	"encoding/json"
	"faydfs/config"
	message2 "faydfs/datanode/message"
	datanode "faydfs/datanode/service"
	"faydfs/proto"
	"flag"
	"fmt"
	"github.com/shirou/gopsutil/v3/disk"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	conf              = config.GetConfig()
	nameNodeHostURL   = conf.NameNode.NameNodeHost + conf.NameNode.NameNodePort
	heartbeatInterval = conf.DataNode.HeartbeatInterval
)

// GetIP
func GetIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return strings.Split(localAddr.String(), ":")[0]
}

// GetDiskUsage
func GetDiskUsage(path string) uint64 {
	di, err := disk.Usage(path)
	if err != nil {
		fmt.Println(err, "err")
	}
	return di.Free
}

// Global variables as local blockList for all
var blockList = []*proto.BlockLocation{}

// Maintain local Port variables
var localNodeIP string
var localRepIP string

// server
type server struct {
	proto.UnimplementedC2DServer
	proto.UnimplementedN2DServer
}

// GetBlock read chunk
func (s server) GetBlock(mode *proto.FileNameAndMode, stream proto.C2D_GetBlockServer) error {
	b := datanode.GetBlock(mode.FileName, "r")

	// Read all the way to the end and transfer the chunk file in chunks
	for b.HasNextChunk() {
		chunk, n, err := b.GetNextChunk()
		if err != nil {
			return err
		}
		stream.Send(&proto.File{Content: (*chunk)[:n]})
	}
	b.Close()
	return nil
}

// WriteBlock read chunk
func (s server) WriteBlock(blockServer proto.C2D_WriteBlockServer) error {
	fileWriteStream, err := blockServer.Recv()
	if err == io.EOF {
		blockStatus := proto.OperateStatus{Success: false}
		blockServer.SendAndClose(&blockStatus)
	}
	fileName := fileWriteStream.BlockReplicaList.BlockReplicaList[0].BlockName
	b := datanode.GetBlock(fileName, "w")
	fmt.Println(fileWriteStream, "fileWriteStream")
	file := make([]byte, 0)
	for {
		fileWriteStream, err := blockServer.Recv()
		if err == io.EOF {
			fmt.Println("file", string(file))
			b.Close()
			blockStatus := proto.OperateStatus{Success: true}
			blockServer.SendAndClose(&blockStatus)
			break
		}
		content := fileWriteStream.File.Content
		err = b.WriteChunk(content)
		if err != nil {
			blockStatus := proto.OperateStatus{Success: false}
			blockServer.SendAndClose(&blockStatus)
		}
		file = append(file, content...)
	}
	fmt.Println("write success")
	// renew List
	blockList = append(blockList,
		&proto.BlockLocation{
			BlockName:    fileName,
			IpAddr:       localNodeIP,
			BlockSize:    b.GetFileSize(),
			ReplicaState: proto.BlockLocation_ReplicaCommitted,
			ReplicaID:    1,
		})
	return nil
}

// ReloadReplica
func (s server) ReloadReplica(ctx context.Context, info *proto.CopyReplica2DN) (*proto.OperateStatus, error) {

	new := info.NewIP[len(info.NewIP)-1:]
	err := ReplicateBlock(info.BlockName, "localhost:5000"+new)
	if err != nil {
		return &proto.OperateStatus{Success: false}, err
	}
	return &proto.OperateStatus{Success: true}, nil
}

// GetBlockReport
func (s server) GetBlockReport(ctx context.Context, ping *proto.Ping) (*proto.BlockReplicaList, error) {
	return &proto.BlockReplicaList{BlockReplicaList: blockList}, nil
}

func heartBeat(currentPort string) {
	heartbeatDuration := time.Second * time.Duration(heartbeatInterval)
	time.Sleep(heartbeatDuration)
	conn, err := grpc.Dial(nameNodeHostURL, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	// defer conn.Close()
	c := proto.NewD2NClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	beat := proto.Heartbeat{IpAddr: currentPort, DiskUsage: GetDiskUsage("D:/")}
	response, err := c.DatanodeHeartbeat(ctx, &beat)
	if err != nil {
		log.Fatalf("did not send heartbeat: %v", err)
	}
	fmt.Printf("response from %v\n", response)
	heartBeat(currentPort)
}

func blockReport() {
	heartbeatDuration := time.Second * time.Duration(heartbeatInterval)
	time.Sleep(heartbeatDuration * 20)
	conn, err := grpc.Dial(nameNodeHostURL, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	// defer conn.Close()
	c := proto.NewD2NClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// add blockList
	response, err := c.BlockReport(ctx, &proto.BlockReplicaList{BlockReplicaList: blockList})

	if err != nil {
		log.Fatalf("did not send heartbeat: %v", err)
	}
	fmt.Println(response)
	blockReport()
}

// register DataNode
func registerDataNode(currentPort string) error {
	fmt.Println("register")
	conn, err := grpc.Dial(nameNodeHostURL, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
		return err
	}
	defer conn.Close()
	c := proto.NewD2NClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	registerStatus, err := c.RegisterDataNode(ctx, &proto.RegisterDataNodeReq{New: true, DiskUsage: GetDiskUsage("D:/"), IpAddr: currentPort})
	if err != nil {
		log.Fatalf("did not register: %v", err)
		return err
	}
	fmt.Println(registerStatus, "registerStatus")
	go heartBeat(currentPort)
	go blockReport()
	return nil
}

// PipelineServer Replicate the datanode to another
func PipelineServer(currentPort string) {
	localRepIP = currentPort
	fmt.Println("start server...")
	var mu sync.Mutex
	//Create a lock to prevent the coroutine from writing the connection's transfer into the same file
	listener, err := net.Listen("tcp", currentPort)
	if err != nil {
		fmt.Println("listen failed,err:", err)
		return
	}
	//Accept client information
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("accept failed,err:", err)
			continue
		}
		//Establish a connection using a coroutine
		go process(conn, mu)
	}
}

// process DataNode
func process(conn net.Conn, mu sync.Mutex) {
	mu.Lock() //Concurrency safety, preventing coroutines from writing at the same time
	defer mu.Unlock()
	defer conn.Close()
	for {
		buf := make([]byte, 10240)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("read err:", err)
			return
		}
		var message message2.Message
		err = json.Unmarshal(buf[0:n], &message)
		if err != nil {
			fmt.Println("unmarshal error: ", err)
		}
		if message.Mode == "send" {
			ReplicateBlock(message.BlockName, message.IpAddr)
		} else if message.Mode == "receive" {
			ReceiveReplicate(message.BlockName, message.Content)
		} else if message.Mode == "delete" {
			err := os.Remove(*datanode.DataDir + "/" + message.BlockName)
			if err != nil {
				return
			}
		}
	}
}

// ReplicateBlock Block secondary node sends backup files to the new node
func ReplicateBlock(blockName string, ipAddress string) error {

	conn, err := net.DialTimeout("tcp", ipAddress, 5*time.Second)
	defer conn.Close()
	if err != nil {
		fmt.Println("Error dialing", err.Error())
		return err
	}
	b := datanode.GetBlock(blockName, "r")
	// Build Message object and serialize
	m := message2.Message{Mode: "receive", BlockName: blockName, Content: b.LoadBlock()}
	mb, err := json.Marshal(m)
	if err != nil {
		fmt.Println("Error marshal", err.Error())
		return err
	}
	// transfer data
	conn.Write(mb)
	return nil
}

// ReceiveReplicate
func ReceiveReplicate(blockName string, content []byte) {
	log.Println("DataNode2接受到DataNode1数据，在本地有相关block备份")
	b := datanode.GetBlock(blockName, "w")
	b.Write(content)
}

// DeleteReplicate
func DeleteReplicate(blockName string) {
	b := datanode.GetBlock(blockName, "w")
	b.DeleteBlock()
}

// RunDataNode
func RunDataNode(currentPort string) {
	localNodeIP = currentPort
	lis, err := net.Listen("tcp", currentPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	err = registerDataNode(currentPort)
	if err != nil {
		log.Fatalf("failed to regester to namenode: %v", err)
	}
	proto.RegisterC2DServer(s, &server{})
	proto.RegisterN2DServer(s, &server{})
	err = s.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// start DataNode
func main() {
	datanode.DataDir = flag.String("dir", "", "the directory be used to store file section")
	dnAddr := flag.String("dn_addr", "", "dataNode server address")
	pplAddr := flag.String("ppl_addr", "", "pipeline server address")
	flag.Parse()
	if len(*datanode.DataDir) == 0 {
		fmt.Fprintf(os.Stderr, "dir must be init")
		os.Exit(1)
	}
	if len(*dnAddr) == 0 {
		fmt.Fprintf(os.Stderr, "dn_addr must be init")
		os.Exit(1)
	}
	if len(*pplAddr) == 0 {
		fmt.Fprintf(os.Stderr, "ppl_addr must be init")
		os.Exit(1)
	}
	// Create new data folder
	os.Mkdir(*datanode.DataDir, 7050)

	// Start the DataNode interactive service
	go PipelineServer(*pplAddr)
	// go PipelineServer("localhost:50001")
	// Open several DataNodes locally
	go RunDataNode(*dnAddr)
	//go RunDataNode("localhost:8011")

	defer func() {
		select {}
	}()
}
