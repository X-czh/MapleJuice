package filesystem

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strings"
	"time"
)

func getRPCClient(ip string) (*rpc.Client, error) {
	service := ip + ":1235"
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

func getFileSize(bytes int64) string {
	size := [4]string{"B", "KB", "MB", "G"}
	idx := 0
	for (bytes / 1000) > 1 {
		idx++
		bytes /= 1000
	}

	bytesStr := fmt.Sprintf("%v", bytes)
	return bytesStr + size[idx]
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
