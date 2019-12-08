package main

import (
	"bytes"
	"fmt"
	"log"
	"os/exec"
	// "strings"
)

func main() {
	var cmdArgs []string
	cmdArgs = append(cmdArgs, "run")
	cmdArgs = append(cmdArgs, "juice_wordcount.go")
	cmdArgs = append(cmdArgs, "aaa")
	cmdArgs = append(cmdArgs, "1\n1\n")
	cmd := exec.Command("go", cmdArgs...)
	// cmd.Stdin = strings.NewReader("some input")
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		fmt.Println(fmt.Sprint(err) + ": " + stderr.String())
		log.Fatal(err)
	}
	fmt.Printf("in all caps: %q \n", out.String())
	fmt.Println(out.String())
}