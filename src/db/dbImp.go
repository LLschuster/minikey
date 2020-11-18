package db

/*
Idea to implement

Have all keys in a hashmap in memory, they will point to a an offset and content length(in case of primitive values) or a file(in the case of a blob)
when reading a key search on the table, then open corresponding file (search by the offset) or just send the whole file (blobs)
maintain a log in disk of our hashmap, in case we need to recover it.
*/

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	Primitive int = iota
	Blob
)

type memtable struct {
	data      map[string]string
	mutex     *sync.Mutex
	lockowner int
}

var mutex sync.Mutex
var keysStore memtable = memtable{nil, &mutex, 0}

//DBinit is the database implementation
func DBinit() {
	keysStore.data = RecoverMemTableFromFiles() //make(map[string]string)
	keysStore.data["offset"] = fmt.Sprintf("%d", 0)

	file, err := os.OpenFile("files/keyvalue.txt", os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(fmt.Sprintf("Could not create keyvalue disk file : %v", err))
		os.Exit(2)
	}
	file.Close()
	fmt.Println("init db implementation")
}

//InsertKey allows you to insert a new to key to the db
func InsertKey(key string, value string, fileType int) (result bool, err error) {
	if keysStore.data == nil {
		return false, errors.New("keysStore is null, Did you initialize the db?")
	}

	requestID := rand.Intn(999999) + 2
	keysStore.mutex.Lock()
	keysStore.lockowner = requestID
	unlockTable := func() {
		if requestID == keysStore.lockowner {
			keysStore.lockowner = 0
			keysStore.mutex.Unlock()
		}
	}
	defer unlockTable()
	time.AfterFunc(10*time.Second, unlockTable)

	if fileType == Primitive {
		file, err := os.OpenFile("files/keyvalue.txt", os.O_RDWR, 0644)
		if err != nil {
			fmt.Printf("%v ", err)
			panic("keyvalue file could not be open")
		}
		defer file.Close()

		offset, err := strconv.ParseInt(keysStore.data["offset"], 10, 64)

		contentLen, err := file.WriteAt([]byte(fmt.Sprintf("%s:%s\n", key, value)), offset)
		if err != nil {
			fmt.Printf("%v ", err)
			panic(fmt.Sprintf("Could not write new key : %v", err))
		}

		keysStore.data[key] = fmt.Sprintf("%d|%d", offset, contentLen)

		offset += int64(contentLen)
		keysStore.data["offset"] = fmt.Sprintf("%d", offset)
	}
	fmt.Println("finish inserting")
	return true, nil
}

//RecoverMemTableFromFiles will take the files on disk
//populate the memtable with the keys
//compress the file, this means repeated keys will be remove until the latest one
//populate the new offsets to the memtable
func RecoverMemTableFromFiles() map[string]string {
	recoverTable := make(map[string]string)
	//amegabyte := 1024 * 1024

	file, err := os.OpenFile("files/keyvalue.txt", os.O_RDWR, 0644)
	if err != nil {
		fmt.Println("There are no files, start empty")
		return recoverTable
	}
	defer file.Close()

	compressFile, err := os.Create("files/temp.txt")
	if err != nil {
		fmt.Println("This should not happen")
		panic("ups: ERROR 829")
	}
	defer compressFile.Close()

	currentReadOffset, err := file.Seek(1, 2)
	toRead := make([]byte, currentReadOffset/4)

	for lines, _ := file.ReadAt(toRead, currentReadOffset-int64(len(toRead))); lines > 0; {
		i := bytes.LastIndexByte(toRead, '\n')
		if i+1 >= lines {
			toRead = toRead[:i]
			i = bytes.LastIndexByte(toRead, '\n')
		}
		if i == -1 && currentReadOffset > int64(len(toRead)) {
			newLen := 2 * len(toRead)
			if int64(newLen) >= currentReadOffset {
				newLen = int(currentReadOffset)
			}
			toRead = make([]byte, newLen)
			continue
		}
		stringthing := string(toRead[:])
		fmt.Println(stringthing)

		ckey := toRead[i+1:]
		_, err = compressFile.Write(ckey)
		fmt.Println(err)

		minusOffset := len(ckey)
		currentReadOffset = currentReadOffset - int64(minusOffset)
		if currentReadOffset <= int64(len(toRead)) {
			break
		}
		toRead = make([]byte, currentReadOffset/4)
	}
	return recoverTable
}
