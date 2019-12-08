package main

import (
	"bytes"
	"fmt"
	"log"
	"os/exec"
	// "strings"
)

func main() {
	cmd := exec.Command("go", "run", "maple_wordcount.go", "aaa aaa")
	// cmd.Stdin = strings.NewReader("some input")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("in all caps: %q\n", out.String())
	fmt.Println(out.String())
}
