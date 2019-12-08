package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage:", os.Args[0], "-string")
		os.Exit(1)
	}

	urls := strings.Fields(os.Args[1])

	for _, url := range urls {
		url = strings.ReplaceAll(url, "/", "_")
		fmt.Println(url + " 1")
	}
}
