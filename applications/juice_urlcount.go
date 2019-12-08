package main

import (
	"bytes"
	"fmt"
	"os"
	// "io"
	"os/exec"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage:", os.Args[0], "-key -filename")
		os.Exit(1)
	}

	filename := os.Args[2]
	command := "wc -l < " + filename
	cmd := exec.Command("bash", "-c", command)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Run()

	outStr := out.String()
	if len(outStr) > 1 {
		fmt.Println(outStr[:len(outStr)-1])
	} else {
		fmt.Println(outStr[:len(outStr)])
	}
}
