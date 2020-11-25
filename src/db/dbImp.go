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
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
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

type ReadKeyResponse struct {
	Value string
	Err   error
}

var mutex sync.Mutex
var keysStore memtable = memtable{nil, &mutex, 0}

//DBinit is the database implementation
func DBinit() {
	keysStore.data = make(map[string]string)
	keysStore.data["offset"] = fmt.Sprintf("%d", 0)

	file, err := os.OpenFile("files/keyvalue.txt", os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(fmt.Sprintf("Could not create keyvalue disk file : %v", err))
		os.Exit(2)
	}
	file.Close()
	fmt.Println("init db implementation")
}

func GetKey(key string, c chan ReadKeyResponse) {
	if keysStore.data == nil {
		c <- ReadKeyResponse{"", errors.New("keysStore is null, Did you initialize the db?")}
	}

	if value, ok := keysStore.data[key]; ok == true {
		fileMetadata := strings.Split(value, "|")
		fileName := fileMetadata[0]
		contentOffset, _ := strconv.ParseInt(fileMetadata[1], 10, 32)
		contentLen, _ := strconv.ParseInt(fileMetadata[2], 10, 64)

		file, err := os.OpenFile(fmt.Sprintf("files/%s", fileName), os.O_RDWR, 0644)
		if err != nil {
			c <- ReadKeyResponse{"", errors.New("error opening the db")}
		}
		readBuffer := make([]byte, contentLen)
		lines, _ := file.ReadAt(readBuffer, contentOffset)
		if lines <= 0 {
			c <- ReadKeyResponse{"", errors.New("error opening the db")}
		}
		c <- ReadKeyResponse{string(readBuffer), nil}

	}

	c <- ReadKeyResponse{"", errors.New("This key is not present in the database")}
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
		file, fileName, err := getCurrentFile()
		if err != nil {
			fmt.Printf("%v ", err)
			panic("keyvalue file could not be open")
		}
		defer file.Close()

		offset := getFileSize(file)

		contentLen, err := file.Write([]byte(fmt.Sprintf("%s:%s:%s\n", key, value, fileName)))
		if err != nil {
			fmt.Printf("%v ", err)
			panic(fmt.Sprintf("Could not write new key : %v", err))
		}

		keysStore.data[key] = fmt.Sprintf("%s|%d|%d", fileName, offset, contentLen)

	}
	fmt.Println("finish inserting")
	return true, nil
}

func getCurrentFile() (*os.File, string, error) {
	cmd := exec.Command("ls", "-c", "files")
	queryFileNames, err := cmd.Output()
	if err != nil {
		fmt.Printf("error %v", err)
		return nil, "", errors.New("Could not get a valid file")
	}

	fileNames := strings.Split(string(queryFileNames), " ")
	fmt.Printf("%v %v \n", fileNames, queryFileNames)

	for _, fileName := range fileNames {
		fmt.Printf("%v", fileName)
		file, err := os.OpenFile(fmt.Sprintf("files/%s", fileName), os.O_APPEND, 0644)
		if err != nil {
			continue
		}

		size := getFileSize(file)

		if size < 200 {
			return file, fileName, nil
		}
	}

	newFileName := uuid.New()
	file, err := os.Create(fmt.Sprintf("files/%s", newFileName.String()))
	if err != nil {
		return nil, "", errors.New("Could not create new file")
	}
	return file, newFileName.String(), nil
}

func getFileSize(file *os.File) int64 {
	finfo, _ := file.Stat()
	return finfo.Size()
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
