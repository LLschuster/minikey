package main

import (
	"fmt"
	"os"
	"os/exec"
)

func main() {
	cmd := exec.Command("ls", "-c", "files")
	filesNames, err := cmd.Output()
	if err != nil {
		fmt.Printf("error %v", err)
	}
	fmt.Printf("%v %T \n", string(filesNames), filesNames)

	file, _ := os.OpenFile("files/keyvalue.txt", os.O_RDWR, 0644)
	finfo, _ := file.Stat()
	size := finfo.Size()

	fmt.Printf("file size %v ", size)
}
