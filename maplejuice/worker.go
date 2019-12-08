package maplejuice

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"strconv"
	"strings"

	F "../filesystem"
)

// WorkerService provides services of MapleJuice's worker
type WorkerService struct {
	fs *F.FileSystemService
}

type JuiceChan struct {
	fileName string
	index    int
}

// StartService starts master service of MapleJuice
func (worker *WorkerService) StartService(fs *F.FileSystemService) error {
	// initialize WorkerService
	worker.fs = fs

	// setup RPC service
	rpc.Register(worker)
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":1236")
	if err != nil {
		return err
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return err
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()

	fmt.Println("[worker] started")
	return nil
}

func (worker *WorkerService) SubmitMaple(strList []string) error {
	job := new(Job)
	job.Type = 0 // Maple
	job.Exe = strList[1]
	numWorker, err := strconv.Atoi(strList[2])
	if err != nil || numWorker <= 0 {
		return errors.New("<num_maples> should be a valid positive integer")
	}
	job.NumWorkers = numWorker
	job.IntermediateFilePrefix = strList[3]
	job.SrcDirectory = strList[4:]

	masterIP := "172.22.156.24:1236"
	client, err := rpc.Dial("tcp", masterIP)
	checkError(err)

	var reply int
	log.Println("submitted maple to master")
	err = client.Call("MasterService.HandleMapleRequest", job, &reply)
	if err != nil {
		return errors.New("submit maple request failed")
	}

	fmt.Println("[INFO] job submitted")

	return nil
}

func (worker *WorkerService) SubmitJuice(strList []string) error {
	job := new(Job)
	job.Type = 1 // Juice
	job.Exe = strList[1]
	numWorker, err := strconv.Atoi(strList[2])
	if err != nil || numWorker <= 0 {
		return errors.New("<num_juices> should be a valid positive integer")
	}
	job.NumWorkers = numWorker
	job.IntermediateFilePrefix = strList[3]
	job.DestFile = strList[4]
	intDeleteInput, err := strconv.Atoi(strList[5])
	if err != nil || (intDeleteInput != 0 && intDeleteInput != 1) {
		return errors.New("<delete_input> can only be 0 or 1")
	}
	if intDeleteInput == 1 {
		job.DeleteInput = true
	} else {
		job.DeleteInput = false
	}
	job.Partitioner = "range"
	if len(strList) == 7 {
		job.Partitioner = strList[7]
	}

	masterIP := "172.22.156.24:1236"
	client, err := rpc.Dial("tcp", masterIP)
	checkError(err)

	var reply int
	log.Println("submitted juice to master")
	err = client.Call("MasterService.HandleJuiceRequest", job, &reply)
	if err != nil {
		return errors.New("submit juice request failed")
	}

	fmt.Println("[INFO] job submitted!")

	return nil
}

func (worker *WorkerService) RunMapleTask(job *Job, reply *int) error {
	// delete existing temp folder and create a new one for storing temp files
	os.RemoveAll("temp")
	os.Mkdir("temp", 0777)

	fmt.Println("Start running Maple task!")

	files := job.SrcDirectory
	filepath := "sdfs/"
	exepath := "./sdfs/"

	// look for exe on sdfs
	exe := "maple_" + job.Exe
	if !worker.fs.Contains(exe) {
		log.Printf("[worker] doesn't have exe file %s, call GET", exe)
		worker.fs.Get(exe, "temp/"+exe)
		exepath = "./temp/"
	}

	keyValueMap := make(map[string][]string)

	for _, fileName := range files {
		if !worker.fs.Contains(fileName) {
			log.Printf("[worker] doesn't have file %s, call GET", fileName)
			worker.fs.Get(fileName, "temp/"+fileName)
			filepath = "temp/"
		}
		file, err := os.Open(filepath + fileName)
		checkError(err)
		defer file.Close()

		scanner := bufio.NewScanner(file)
		count := 0
		buf := bytes.Buffer{}
		for scanner.Scan() {
			count++
			buf.WriteString(scanner.Text())
			buf.WriteString("\n")
			if count%10 == 0 {
				log.Println("calling a new maple exe")
				out := ExecuteMaple(exepath+exe, buf.String())
				outStr := out.String()
				keyValuePairs := strings.Split(outStr, "\n")
				for _, pair := range keyValuePairs {
					if len(pair) == 0 {
						continue
					}
					keyValue := strings.Split(pair, " ")
					// log.Printf("key: %v, len:%d", keyValue[0], len(keyValue[0]))
					key := keyValue[0]
					value := keyValue[1]
					keyValueMap[key] = append(keyValueMap[key], value)
					// keys = append(keys, key)
					// values = append(values, value)
				}
				count = 0
				buf = bytes.Buffer{}
			}
		}

		if count != 0 {
			log.Println("calling a new maple exe")
			out := ExecuteMaple(exepath+exe, buf.String())
			outStr := out.String()
			keyValuePairs := strings.Split(outStr, "\n")
			for _, pair := range keyValuePairs {
				// fmt.Println(pair)
				if len(pair) == 0 {
					continue
				}
				keyValue := strings.Split(pair, " ")
				// log.Printf("key: %v, len:%d", keyValue[0], len(keyValue[0]))
				key := keyValue[0]
				value := keyValue[1]
				keyValueMap[key] = append(keyValueMap[key], value)
				// keys = append(keys, key)
				// values = append(values, value+"\n")
			}
		}
	}

	keys := make([]string, len(keyValueMap))
	values := make([]string, len(keyValueMap))
	i := 0
	for k, v := range keyValueMap {
		keys[i] = k
		vStr := strings.Join(v, "\n")
		values[i] = vStr
		i++
	}

	var mapleResult JobResult
	mapleResult.Keys = keys
	mapleResult.Results = values

	masterIP := "172.22.156.24:1236"
	client, err := rpc.Dial("tcp", masterIP)
	checkError(err)

	var replyFromMaster int
	log.Println("submitted maple result to master")

	err = client.Call("MasterService.HandleMapleResult", mapleResult, &replyFromMaster)
	checkError(err)

	fmt.Println("Finish running Maple task!")
	return nil
}

func ExecuteMaple(exe string, content string) bytes.Buffer {
	cmd := exec.Command(exe, content)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	checkError(err)
	return out
}

func (worker *WorkerService) RunJuiceTask(job *Job, reply *int) error {
	// delete existing temp folder and create a new one for storing temp files
	os.RemoveAll("temp")
	os.Mkdir("temp", 0777)

	fmt.Println("Start running Juice task!")

	files := job.SrcDirectory
	filepath := "sdfs/"
	filesLen := len(files)
	exepath := "./sdfs/"
	var juiceResult JobResult
	juiceResult.Keys = make([]string, filesLen)
	juiceResult.Results = make([]string, filesLen)

	// look for exe on sdfs
	exe := "juice_" + job.Exe
	if !worker.fs.Contains(exe) {
		log.Printf("[worker] doesn't have exe file %s, call GET", exe)
		worker.fs.Get(exe, "temp/"+exe)
		exepath = "./temp/"
	}

	prefixLen := len(job.IntermediateFilePrefix)
	for i, fileName := range files {
		log.Printf("[worker] reading %s", fileName)
		if !worker.fs.Contains(fileName) {
			log.Printf("[worker] doesn't have file %s, call GET", fileName)
			worker.fs.Get(fileName, "temp/"+fileName)
			filepath = "temp/"
		}

		key := fileName[prefixLen:]
		var cmdArgs []string
		cmdArgs = append(cmdArgs, key)
		cmdArgs = append(cmdArgs, filepath+fileName)
		out := ExecuteJuice(exepath+exe, cmdArgs)
		value := out.String()

		juiceResult.Keys[i] = key
		juiceResult.Results[i] = value
	}

	masterIP := "172.22.156.24:1236"
	client, err := rpc.Dial("tcp", masterIP)
	checkError(err)

	var replyFromMaster int
	log.Println("submitted juice result to master")
	err = client.Call("MasterService.HandleJuiceResult", juiceResult, &replyFromMaster)
	checkError(err)

	fmt.Println("Finish running Juice task!")
	return nil
}

func ExecuteJuice(exe string, cmdArgs []string) bytes.Buffer {
	cmd := exec.Command(exe, cmdArgs...)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		fmt.Println(fmt.Sprint(err) + ": " + stderr.String())
	}
	return out
}
