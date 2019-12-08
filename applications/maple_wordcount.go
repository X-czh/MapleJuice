package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 || len(os.Args) > 2 {
		fmt.Println("Usage:", os.Args[0], "-string")
		os.Exit(1)
	}

	words := strings.Fields(os.Args[1])

	for _, word := range words {
		fmt.Println(word + " 1")
	}
}
