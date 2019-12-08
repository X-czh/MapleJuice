package maplejuice

import (
	"fmt"
	"hash/fnv"
	"math"
	"net"
	"net/rpc"
	"os"
	"strings"
	"time"
)

// Job is a generic representation of a Maple or Juice job
type Job struct {
	Type                   int // 0 for Maple, 1 for Juice
	Exe                    string
	IntermediateFilePrefix string
	SrcDirectory           []string
	NumWorkers             int
	DestFile               string
	DeleteInput            bool
	Partitioner            string
}

// JobResult is a generic representation of the result of a Maple or Juice task
type JobResult struct {
	Keys    []string
	Results []string
}

// Range Partitioner for Juice
func rangePartition(src []string, num int) [][]string {
	// precondition: num >= len(src); src is sorted

	result := make([][]string, num)

	// partition by range
	batch := int(math.Ceil(float64(len(src) / num)))
	for i := 0; i < num-1; i++ {
		result[i] = src[batch*i : batch*(i+1)]
	}
	result[num-1] = src[batch*(num-1):]

	return result
}

// Hash Partitioner for Juice
func hashPartition(src []string, num int) [][]string {
	// precondition: num >= len(src)

	result := make([][]string, num)

	for _, str := range src {
		h := fnv.New32a()
		h.Write([]byte(str))
		mod := int(h.Sum32()) % num
		result[mod] = append(result[mod], str)
	}

	return result
}

func getRPCClient(ip string) (*rpc.Client, error) {
	service := ip + ":1236"
	conn, err := net.DialTimeout("tcp", service, 10*time.Second)
	if err != nil {
		return nil, err
	}

	client := rpc.NewClient(conn)
	return client, nil
}

func getIPfromID(id string) string {
	idArr := strings.Split(id, ":")
	return idArr[0]
}

// check error and print error info to user
func checkError(err error) {
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
}

// check fatal error and print error info to user
func checkFatalError(err error) {
	if err != nil {
		fmt.Printf("Fatal error: %v\n", err)
		os.Exit(1)
	}
}
