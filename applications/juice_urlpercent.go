package main

import (
	"fmt"
	"os"
	"strings"
	// "math"
	"bufio"
	"strconv"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage:", os.Args[0], "-key -string")
		os.Exit(1)
	}

	fileName := os.Args[2]
	file, err := os.Open(fileName)

	if err != nil {
		panic(err)
	}

	total := 0
	scanner := bufio.NewScanner(file)
	urlMap := make(map[string]int)
	
	for scanner.Scan() {
		if len(scanner.Text()) == 0 {
			continue
		}
		pair := strings.Split(scanner.Text(), "-")
		count, err := strconv.Atoi(pair[0])
		if err != nil {
			panic(err)
		}
		url := strings.ReplaceAll(pair[1], "_", "/")
		urlMap[url] = count
		total += count
	}

	for k, v := range urlMap {
		fmt.Printf(k + " %.2f %%\n", float32(v) / float32(total) * float32(100))
	}
}