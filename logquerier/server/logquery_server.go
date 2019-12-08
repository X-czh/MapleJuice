package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"reflect"
	"strconv"
)

// paths of log files to query
var log_files []string

type LogGen int

func RandomIP() string {
	return strconv.Itoa(rand.Intn(256)) + "." + strconv.Itoa(rand.Intn(256)) + "." + strconv.Itoa(rand.Intn(256)) + "." + strconv.Itoa(rand.Intn(256))
}

func (t *LogGen) GenerateLog(id int, reply *int) error {
	var logs string

	// unique pattern for every machine
	logs += "log file for vm" + strconv.Itoa(id+1) + "\n"
	// frequent pattern
	for i := 0; i < 100; i++ {
		logs += "vm" + strconv.Itoa(id+1) + " dialing " + RandomIP() + "\n"
	}
	// a little infrequent pattern
	for i := 0; i < 30; i++ {
		logs += "vm" + strconv.Itoa(id+1) + " disconnected from " + RandomIP() + "\n"
	}
	// rare pattern
	if (id+1)%3 == 1 {
		logs += "NEED MAINTANENCE"
	}

	// generate log file
	logFileName := "vm" + strconv.Itoa(id+1) + "-test.log"
	err := ioutil.WriteFile(logFileName, []byte(logs), 0644)
	if err != nil {
		log.Fatal(err)
	}

	// register log files for server
	log_files = []string{logFileName}

	*reply = 1
	return nil
}

type LogQuery int

func (t *LogQuery) Grep(args []string, reply *string) error {
	// execute grep
	app := "grep"
	app_args := append(args, log_files...)
	log.Println(app_args)
	cmd := exec.Command(app, app_args...)

	// process grep stdout & stderr and return
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("no pattern matched: %v", err)
		return nil
	}
	*reply = string(out)
	return nil
}

func main() {
	// command line interface
	if len(os.Args) < 3 {
		fmt.Println("Usage:", os.Args[0], "<port_number> <logfile_1> <logfile_2> ... <logfile_N>")
		fmt.Println("Test Mode:", os.Args[0], "<port_number> --test")
		os.Exit(1)
	}
	port_num := os.Args[1]
	if os.Args[2] == "--test" {
		log.Printf("Attention: running in test mode!")

		// register log_generate object, publish its methods for RPC
		log_generate := new(LogGen)
		rpc.Register(log_generate)
	} else {
		log_files = os.Args[2:len(os.Args)]
	}

	// register log_query object, publish its methods for RPC
	log_query := new(LogQuery)
	rpc.Register(log_query)

	// service address of server
	service := ":" + port_num

	// create tcp address
	tcpAddr, err := net.ResolveTCPAddr("tcp", service)
	if err != nil {
		log.Fatal(err)
	}

	// create tcp listener
	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatal(err)
	}

	// server started now
	log.Printf("server started")

	// forever loop to listen for and accept client connections
	for {
		// wait for incoming client connections
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("connection error: %v", err)
			os.Exit(1)
		}

		// received rpc message, pass to rpc handler
		log.Printf("received rpc message %v %v", reflect.TypeOf(conn), conn)
		go rpc.ServeConn(conn)
	}
}
