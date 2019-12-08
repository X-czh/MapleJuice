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

var record_nums []int

func queryServer(service string, args []string, i int, wg *sync.WaitGroup) {
	// thread unblock signal
	defer wg.Done()

	// allow a 5s dial
	max, _ := time.ParseDuration("5s")
	conn, err := net.DialTimeout("tcp", service, max)
	if err != nil {
		fmt.Printf("[connection error] vm%d not responding\n", i)
		record_nums[i] = -1
		return
	}

	client := rpc.NewClient(conn)
	var reply string
	err = client.Call("LogQuery.Grep", args, &reply)
	if err != nil {
		log.Fatal("grep:", err)
	}

	// write output to file
	fileName := "vm" + strconv.Itoa(i+1) + "output.txt"
	err = ioutil.WriteFile(fileName, []byte(reply), 0644)
	if err != nil {
		log.Fatal(err)
	}

	record_nums[i] = strings.Count(string(reply), "\n")
}

func logGenerateServer(service string, i int, wg *sync.WaitGroup) error {
	// allow a 5s dial
	defer wg.Done()
	max, _ := time.ParseDuration("5s")
	conn, err := net.DialTimeout("tcp", service, max)
	if err != nil {
		fmt.Printf("[connection error] vm%d not responding\n", i)
		record_nums[i] = -1
		log.Fatal(err)
	}

	client := rpc.NewClient(conn)
	var reply int
	err = client.Call("LogGen.GenerateLog", i, &reply)
	if err != nil {
		log.Fatal("GenerateLog:", err)
	}

	if reply == 1 {
		fmt.Printf("[vm%d] generated log file\n", i+1)
	}

	return nil
}

func TestGrep(services []string, num int) {
	tables := []struct {
		pattern string
		occur   int // total occurrences across lo files
	}{
		{"NEED MAINTANENCE", num/3 + 1},
		{"log file", num},
		{"dialing", 100 * num},
	}

	var wg sync.WaitGroup
	sum := 0

	for k, table := range tables {
		wg.Add(num)

		for i, service := range services {
			args := []string{table.pattern, "-EnH"}
			go queryServer(service, args, i, &wg)
		}

		wg.Wait()

		sum = 0
		for _, num := range record_nums {
			sum += num
		}

		if sum != table.occur {
			fmt.Printf("[Test%d] pattern %s is wrong, got: %d, want %d\n", k, table.pattern, sum, table.occur)
		} else {
			fmt.Printf("[Test%d] pattern %s is correct, got: %d, want %d\n", k, table.pattern, sum, table.occur)
		}

	}

}

func main() {
	// // command line interface
	if len(os.Args) < 2 || os.Args[1] == "-h" {
		fmt.Println("Usage:", os.Args[1], "number of vm")
		os.Exit(1)
	}

	// get number of vm
	num, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	record_nums = make([]int, num)

	// read ip addr & port num
	content, err := ioutil.ReadFile("server_list.config")
	if err != nil {
		log.Fatal("fail to open server_list.config")
	}
	services := strings.Split(string(content), "\n")[0:num]

	// do query with go
	// use threading to query every vm
	var wg sync.WaitGroup

	wg.Add(num)

	for i, service := range services {
		go logGenerateServer(service, i, &wg)
	}

	wg.Wait()

	TestGrep(services, num)

}
