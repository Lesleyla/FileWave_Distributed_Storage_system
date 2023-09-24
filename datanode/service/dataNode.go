package service

import (
	"bufio"
	"bytes"
	"errors"
	"faydfs/config"
	"fmt"
	"io"
	"log"
	"os"
)

// Block struct provides api to read and write block
type Block struct {
	blockName string
	offset    int
	chunkSize int
	reader    *bufio.Reader
	buffer    *[]byte
	dataRead  int
	file      *os.File
	blockSize int64
}

// read config
var conf = config.GetConfig()
var DataDir *string

func (b *Block) initBlock(blockName string, mode string) {
	var err error
	var reader *bufio.Reader 
	var file *os.File
	if mode == "r" {
		file, err = os.Open(*DataDir + "/" + blockName)
		reader = bufio.NewReader(file)
		// Write mode initialization
	} else if mode == "w" {
		file, err = os.OpenFile(*DataDir+"/"+blockName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	}
	if err != nil {
		log.Fatal("cannot open image file: ", err)
	}
	b.file = file
	b.blockName = blockName
	b.reader = reader
	// Parameters set using config files
	b.chunkSize = conf.DataNode.IoSize
	b.blockSize = conf.Block.BlockSize
	// Create a buffer of chunkSize size
	buffer := make([]byte, b.chunkSize)
	b.buffer = &buffer
	// When dataRead = -1, it means that EOF has not been reached and you can continue reading
	b.dataRead = -1
	b.offset = 0
}

// GetBlock
func GetBlock(blockName string, mode string) *Block {
	// initialize struct
	block := Block{}
	block.initBlock(blockName, mode)
	return &block
}

// HasNextChunk
func (b *Block) HasNextChunk() bool {
	if b.dataRead != -1 {
		return true
	}
	// Read a piece of data of chunkSize size from the file into the buffer of the block
	n, err := b.reader.Read(*b.buffer)
	if err == io.EOF {
		b.dataRead = -1
		b.file.Close()
		return false
	}
	// read error
	if err != nil {
		log.Fatal("cannot read chunk to buffer: ", err)
	}
	// dataRead Set to the number of bytes read
	b.dataRead = n
	return true
}

// GetNextChunk
// error
func (b *Block) GetNextChunk() (*[]byte, int, error) {
	// no chunk
	if b.dataRead == -1 {
		return nil, 0, nil
	}
	//
	n := b.dataRead
	b.dataRead = -1
	return b.buffer, n, nil
}

// WriteChunk
func (b *Block) WriteChunk(chunk []byte) error {
	// Return file status structure variable
	info, err := b.file.Stat()
	if err != nil {
		log.Fatal("cannot open the file: ", err)
	}
	// get file size
	currentBlockSize := info.Size()
	if b.blockSize >= (int64(len(chunk)) + currentBlockSize) {
		_, err := b.file.Write(chunk)
		if err != nil {
			log.Fatal("cannot write to file: ", err)
		}
		return nil
	}
	var ErrFileExceedsBlockSize = errors.New("file is greater than block size")
	return ErrFileExceedsBlockSize
}

// Close
func (b *Block) Close() error {
	return b.file.Close()
}

// GetFileSize
func (b *Block) GetFileSize() int64 {
	file, err := os.Open(*DataDir + "/" + b.blockName)
	if err != nil {
		log.Fatal("error in reading the size: " + err.Error())
	}
	info, err := file.Stat()
	if err != nil {
		log.Fatal("error in reading the size: " + err.Error())
	}
	defer file.Close()
	return info.Size()
}

// LoadBlock
func (b *Block) LoadBlock() []byte {
	var file bytes.Buffer
	sum := 0
	for b.HasNextChunk() {
		chunk, n, err := b.GetNextChunk()
		sum += n
		if err != nil {
			return nil
		}
		file.Write(*chunk)
	}
	b.Close()
	return file.Bytes()[0:sum]
}

// Write block
func (b *Block) Write(content []byte) {
	// default Open
	_, err := b.file.Write(content)
	if err != nil {
		fmt.Printf("write replicate file failed: %v\n", err)
		return
	}
	b.Close()
}

// DeleteBlock
func (b *Block) DeleteBlock() error {
	err := os.Remove(*DataDir + "/" + b.blockName)
	if err != nil {
		return err
	}
	return nil
}
