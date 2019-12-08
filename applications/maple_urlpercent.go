package main

import (
	"fmt"
	"os"
	"strings"
	"math"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage:", os.Args[0], "-string")
		os.Exit(1)
	}

	words := strings.Fields(os.Args[1])
	count := "0"
	url := ""
	counter := 0

	for _, w := range words {
		counter++
		if math.Mod(float64(counter), 2) == 1 {
			count = w
		} else { 
			url = w
			fmt.Println("1 " + url + "-" + count)
		}
	}
}
