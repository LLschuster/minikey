package db

/*
Idea to implement

Have all keys in a hashmap in memory, they will point to a an offset and content length(in case of primitive values) or a file(in the case of a blob)
when reading a key search on the table, then open corresponding file (search by the offset) or just send the whole file (blobs)
maintain a log in disk of our hashmap, in case we need to recover it.
*/

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
)

const (
	Primitive int = iota
	Blob
)

var memtable map[string]string = nil

//DBinit is the database implementation
func DBinit() {
	memtable = make(map[string]string)
	memtable["offset"] = fmt.Sprintf("%d", 0)

	file, err := os.Create("files/keyvalue.txt")
	if err != nil {
		log.Fatal(fmt.Sprintf("Could not create keyvalue disk file : %v", err))
		os.Exit(2)
	}
	file.Close()
	fmt.Println("init db implementation")
}

func InsertKey(key string, value string, fileType int) (result bool, err error) {
	if memtable == nil {
		return false, errors.New("Memtable is null, Did you initialize the db?")
	}

	if fileType == Primitive {
		file, err := os.OpenFile("files/keyvalue.txt", os.O_RDWR, 0644)
		if err != nil {
			fmt.Printf("%v ", err)
			panic("keyvalue file could not be open")
		}
		defer file.Close()

		offset, err := strconv.ParseInt(memtable["offset"], 10, 64)

		contentLen, err := file.WriteAt([]byte(fmt.Sprintf("%s\n", value)), offset)
		if err != nil {
			fmt.Printf("%v ", err)
			panic(fmt.Sprintf("Could not write new key : %v", err))
		}

		memtable[key] = fmt.Sprintf("%d|%d", offset, contentLen)

		offset += int64(contentLen)
		memtable["offset"] = fmt.Sprintf("%d", offset)
	}

	return true, nil
}
