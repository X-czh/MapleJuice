package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var record_nums [10]int

func queryServer(service string, args []string, i int, wg *sync.WaitGroup) {
	// thread unblock signal
	defer wg.Done()

	// allow a 5s dial
	max, _ := time.ParseDuration("5s")
	conn, err := net.DialTimeout("tcp", service, max)
	if err != nil {
		log.Printf("[connection error] vm%d not responding\n", i+1)
		record_nums[i] = -1
		return
	}

	// query log
	client := rpc.NewClient(conn)
	var reply string
	err = client.Call("LogQuery.Grep", args, &reply)
	if err != nil {
		log.Printf("grep: %v", err)
		record_nums[i] = -1
		return
	}

	// write output to file
	fileName := "vm" + strconv.Itoa(i+1) + "output.txt"
	err = ioutil.WriteFile(fileName, []byte(reply), 0644)
	if err != nil {
		log.Printf("cannot open file: %v", err)
	}

	record_nums[i] = strings.Count(string(reply), "\n")
}

func main() {
	// command line interface
	if len(os.Args) < 2 || os.Args[1] == "-h" {
		fmt.Println("Usage:", os.Args[0], "<grep_arg_1> <grep_arg_2> ... <grep_arg_N>")
		os.Exit(1)
	}

	// -nH option to enforce display of the file name & line number of each query entry
	args := append(os.Args[1:len(os.Args)], "-nH")

	// read server ip addr & port num
	content, err := ioutil.ReadFile("server_list.config")
	if err != nil {
		log.Fatal("fail to open server_list.config")
	}
	services := strings.Split(string(content), "\n")

	// do query with go
	// use threading to query every vm
	t1 := time.Now()
	var wg sync.WaitGroup

	wg.Add(len(services))

	for i, service := range services {
		go queryServer(service, args, i, &wg)
	}

	wg.Wait()
	t2 := time.Now()
	diff := t2.Sub(t1)

	sum := 0
	for i, _ := range services {
		if record_nums[i] != -1 {
			fmt.Printf("vm%d: %d lines matched\n", i+1, record_nums[i])
			sum += record_nums[i]
		}
	}
	fmt.Printf("In total, %d lines matched\n", sum)
	fmt.Printf("%v used\n", diff)
}
