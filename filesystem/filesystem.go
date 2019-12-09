package filesystem

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"runtime"
	"sync"
	"time"

	M "../membership"
)

type fileTable struct {
	file2timestamp map[string]string
	file2machines  map[string]map[string]int
	machine2files  map[string]map[string]int
	file2token     map[string]time.Time
}

// FileSystemService provides distributed file management services
type FileSystemService struct {
	Ms              *M.MembershipService
	ft              fileTable
	lock            sync.Mutex
	IsMaster        bool        // am I the master of MapleJuice?
	LeaveReportChan chan string // leave report channel
	FailReportChan  chan string // failure report channel
	JoinReportChan  chan string // join report channel
}

// Args is a joint representation of arguments for RPCs
type Args struct {
	Filename  string
	Content   string // can be empty
	Timestamp string // can be empty
}

// StartService starts file system service
func (fs *FileSystemService) StartService(ms *M.MembershipService) error {
	// initialize FileSystemService
	fs.Ms = ms
	fs.ft.file2timestamp = make(map[string]string)
	fs.ft.file2machines = make(map[string]map[string]int)
	fs.ft.machine2files = make(map[string]map[string]int)
	fs.ft.file2token = make(map[string]time.Time)

	// delete existing sdfs folder and create a new one
	os.RemoveAll("sdfs")
	os.Mkdir("sdfs", 0777)

	// setup RPC service
	rpc.Register(fs)
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":1235")
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

	// start membership update monitors
	fs.IsMaster = false
	fs.LeaveReportChan = make(chan string)
	fs.JoinReportChan = make(chan string)
	fs.FailReportChan = make(chan string)
	go fs.monitorJoin()
	go fs.monitorLeave()
	go fs.monitorFailure()

	return nil
}

// PrePut achieves mutual exclusion before the real put
func (fs *FileSystemService) PrePut(sdfsfilename string) error {
	tokenNum := 0
	args := Args{sdfsfilename, "", ""}
	var mutex sync.Mutex
	var wg sync.WaitGroup
	members := fs.Ms.ListMembers()
	for _, member := range members {
		memberIP := getIPfromID(member)
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := getRPCClient(memberIP)
			checkError(err)
			var reply int
			err = client.Call("FileSystemService.HandleRequestToken", args, &reply)
			log.Println("received response from " + memberIP)
			checkError(err)
			client.Close()
			mutex.Lock()
			if reply == 1 {
				tokenNum++
			}
			mutex.Unlock()

		}()
	}

	wg.Wait()
	log.Println("All response received")
	if tokenNum > len(members)/2 {
		return nil
	}
	log.Printf("didn't receive enough tokens. need: %d, received: %d\n", len(members)/2+1, tokenNum)
	return errors.New("didn't receive enough tokens")
}

// Put inserts or updates the SDFS file with the local file
func (fs *FileSystemService) Put(localfilename string, sdfsfilename string) error {
	timestamp := time.Now().String()

	// read local file content
	data, err := ioutil.ReadFile(localfilename)
	if err != nil {
		log.Printf("[SDFS] <Put> cannot read file/" + localfilename)
		return errors.New("cannot read file/" + localfilename)
	}
	content := string(data)
	args := Args{sdfsfilename, content, timestamp}
	var reply string

	if machineMap, ok := fs.ft.file2machines[sdfsfilename]; !ok {
		log.Printf("[SDFS] <Put> %s not present, insert file\n", sdfsfilename)

		// insert file in self
		fs.HandleInsert(args, &reply)

		// insert in three predecessors to get a total of 4 replicas
		predecessors := fs.Ms.Predecessors
		if len(predecessors) < 3 {
			log.Println("[SDFS] <Put> too few nodes in system, will make fewer replicas")
		}
		var wg sync.WaitGroup
		wg.Add(len(predecessors))
		for _, predecessor := range predecessors {
			go func(wg *sync.WaitGroup, predecessor string) {
				predecessorIP := getIPfromID(predecessor)
				client, err := getRPCClient(predecessorIP)
				if err != nil {
					log.Printf("[SDFS] <Put> dialing %s to getRPCClient failed: %v\n", predecessorIP, err)
					wg.Done()
					return
				}

				err = client.Call("FileSystemService.HandleInsert", args, &reply)
				if err != nil {
					log.Printf("[SDFS] <Put> RPC to HandleInsert on %s failed\n", predecessorIP)
					client.Close()
					wg.Done()
					return
				}

				client.Close()
				wg.Done()
			}(&wg, predecessor)
		}
		wg.Wait()
	} else {
		log.Printf("[SDFS] <Put> %s present, update file\n", sdfsfilename)

		// update file
		var wg sync.WaitGroup
		wg.Add(len(machineMap))
		for machine := range machineMap {
			go func(wg *sync.WaitGroup, machine string) {
				client, err := getRPCClient(machine)
				if err != nil {
					log.Printf("[SDFS] <Put> dialing %s to getRPCClient failed: %v\n", machine, err)
					wg.Done()
					return
				}
				var reply int
				err = client.Call("FileSystemService.HandleUpdate", args, &reply)
				if err != nil {
					log.Printf("[SDFS] <Put> RPC to HandleUpdate on %s failed\n", machine)
					client.Close()
					wg.Done()
					return
				}
				client.Close()
				wg.Done()
			}(&wg, machine)
		}
		wg.Wait()
	}
	return nil
}

// Get retrieves the SDFS file and stores it in the local file
func (fs *FileSystemService) Get(sdfsfilename string, localfilename string) error {
	machineMap, ok := fs.ft.file2machines[sdfsfilename]
	if !ok {
		log.Println("[SDFS] <Get> No machine found that stores the file", sdfsfilename)
		return errors.New("No machine found that stores the file " + sdfsfilename)
	}

	var reply string
	for machineIP := range machineMap {
		client, err := getRPCClient(machineIP)
		if err != nil {
			log.Printf("[SDFS] <Get> dialing %s to getRPCClient failed: %v\n", machineIP, err)
			continue
		}
		args := Args{sdfsfilename, "", ""}
		err = client.Call("FileSystemService.HandleGet", args, &reply)
		client.Close()
		if err == nil {
			break
		} else {
			log.Printf("[SDFS] <Get> RPC to HandleGet on %s failed\n", machineIP)
		}
	}

	// write to local file
	file, err := os.OpenFile(localfilename, os.O_RDWR|os.O_CREATE, 0777)
	defer file.Close()

	// change
	// checkError(err)
	if err != nil {
		log.Printf("open file error, %v\n", err)
	}

	// bytes := []byte(reply)
	l, err := file.WriteString(reply)
	log.Println(l, "bytes written successfully")
	// err := ioutil.WriteFile(localfilename, bytes, 0777)

	// change
	// checkError(err)
	if err != nil {
		log.Printf("write string error, %v\n", err)
	}

	return err
}

// Delete erases the SDFS file
func (fs *FileSystemService) Delete(sdfsfilename string) error {
	selfIP := getIPfromID(fs.Ms.ListSelf())
	args := Args{sdfsfilename, "", ""}
	var reply string
	fs.HandleDelete(args, &reply)

	members := fs.Ms.ListMembers()
	var wg sync.WaitGroup
	wg.Add(len(members))
	for _, member := range members {
		memberIP := getIPfromID(member)
		if memberIP == selfIP {
			wg.Done()
			continue
		}
		go func(wg *sync.WaitGroup) {
			client, err := getRPCClient(memberIP)
			if err != nil {
				fmt.Printf("dial rpc error, %v\n", err)
				return
			}
			err = client.Call("FileSystemService.HandleDelete", args, &reply)
			if err != nil {
				fmt.Printf("call handle delete error, %v\n", err)
			}
			client.Close()
			wg.Done()
		}(&wg)
	}
	wg.Wait()
	return nil
}

// ListMachines lists all machine addresses where this file is currently being stored
func (fs *FileSystemService) ListMachines(sdfsfilename string) ([]string, error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	var res []string
	machineMap, ok := fs.ft.file2machines[sdfsfilename]
	if !ok {
		err := errors.New("No machine found that stores the file " + sdfsfilename)
		return nil, err
	}
	for k := range machineMap {
		res = append(res, k)
	}

	return res, nil
}

// ListFiles lilsts all files currently being stored at this machine
func (fs *FileSystemService) ListFiles() ([]string, error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	var res []string
	for k := range fs.ft.file2timestamp {
		file, err := os.Open("sdfs/" + k)
		if err != nil {
			return nil, err
		}
		stat, err := file.Stat()
		if err != nil {
			return nil, err
		}
		res = append(res, k+"\t"+getFileSize(stat.Size()))
	}
	return res, nil
}

// monitorJoin monitors if a new node has joined the distributed system when the node is the introducer
func (fs *FileSystemService) monitorJoin() {
	for {
		if machine, ok := <-fs.Ms.JoinReportChan; ok {
			log.Println("[SDFS] monitored join of machine ", machine)
			if fs.IsMaster {
				fs.JoinReportChan <- machine
			}
			fs.sendFileMetaData(machine)
		}
		runtime.Gosched()
	}
}

// monitorLeave monitors leaves of group members
func (fs *FileSystemService) monitorLeave() {
	for {
		if machine, ok := <-fs.Ms.LeaveReportChan; ok {
			log.Println("[SDFS] monitored leave of machine ", machine)
			if fs.IsMaster {
				fs.LeaveReportChan <- machine
			}
			fs.reReplicate(machine)
		}
		runtime.Gosched()
	}
}

// monitorFailure monitors failures of group members
func (fs *FileSystemService) monitorFailure() {
	for {
		if machine, ok := <-fs.Ms.FailReportChan; ok {
			log.Println("[SDFS] monitored failure of machine ", machine)
			if fs.IsMaster {
				fs.FailReportChan <- machine
			}
			fs.reReplicate(machine)
		}
		runtime.Gosched()
	}
}

// reReplicate re-replicates files stored on the failed/left machine
func (fs *FileSystemService) reReplicate(machine string) {
	// start := time.Now()
	machineIP := getIPfromID(machine)

	fs.lock.Lock()

	log.Println("rereplicate files from machine", machine)

	// if no files stored on the failed/left machine, nothing needs to be done
	if len(fs.ft.machine2files[machineIP]) == 0 {
		delete(fs.ft.machine2files, machineIP)
		fs.lock.Unlock()
		return
	}

	rand.Seed(time.Now().UnixNano())
	msl := fs.Ms.ListMembers()
	selfIP := getIPfromID(fs.Ms.ListSelf())

	// for all files on the failed/left machine
	for file := range fs.ft.machine2files[machineIP] {
		// first remove failed/left machine from file2machines
		delete(fs.ft.file2machines[file], machineIP)

		// check if I have the file
		if _, ok := fs.ft.file2timestamp[file]; ok {
			log.Println("I have the file", file)

			// if I have the smallest id among all live machines with the file
			// then I am selected to do replication
			selected := true
			for ip := range fs.ft.file2machines[file] {
				if ip < selfIP {
					selected = false
					break
				}
			}
			if selected {
				log.Println("I am selected!")

				finished := false
				for !finished {
					// count number of re-replicas needed, RF = 4
					num := 4 - len(fs.ft.file2machines[file])
					if num < 1 {
						continue
					}

					// candidates for re-replication: all live machines without the file
					candidates := []string{}
					for _, id := range msl {
						ip := getIPfromID(id)

						log.Printf("rereplicate-check if %s has file %s\n", ip, file)

						if _, ok := fs.ft.machine2files[ip][file]; !ok {
							log.Printf("%s doesn't have file %s\n", ip, file)
							candidates = append(candidates, ip)
						}
					}
					log.Println("candidates")
					log.Printf("%v", candidates)

					// prepare for re-replication
					data, err := ioutil.ReadFile("sdfs/" + file)
					if err != nil {
						log.Printf("cannot read file sdfs/" + file)
					}
					content := string(data)
					timestamp := fs.ft.file2timestamp[file]
					fs.lock.Unlock()

					args := Args{file, content, timestamp}
					var reply string

					// randomly select [num] of candiadates to carry out re-replication
					p := rand.Perm(len(candidates))
					log.Println("perm id")
					log.Printf("%v", p)
					failed := false
					for _, r := range p[:num] {
						dest := getIPfromID(candidates[r])

						log.Println("reReplicate to", dest)

						client, err := getRPCClient(dest)
						if err != nil {
							failed = true
							log.Printf("error: %v\n", err)
							break
						}
						err = client.Call("FileSystemService.HandleInsert", args, &reply)
						client.Close()
						if err != nil {
							failed = true
							log.Printf("error: %v\n", err)
							break
						}
					}
					fs.lock.Lock()

					// restart re-replication
					if failed {
						continue
					}

					// rereplicate successfully finished
					finished = true
					log.Println("re-replication succeeds")
				}
			}
		}
	}

	// remove failed/left machine from machine2files
	delete(fs.ft.machine2files, machineIP)

	fs.lock.Unlock()
	// end := time.Now()
	// diff := end.Sub(start).Seconds()
	// fmt.Printf("Time for rereplicate: %v", diff)
}

// sendFileMetaData sends file metadata to newly joined nodes
func (fs *FileSystemService) sendFileMetaData(machine string) error {
	machineIP := getIPfromID(machine)
	metadata := ""
	fs.lock.Lock()
	// compress machine2file in one string
	mCount := 0
	for m := range fs.ft.machine2files {
		if mCount != 0 {
			metadata += "|"
		}
		metadata += m + ":"
		fileCount := 0
		for file := range fs.ft.machine2files[m] {
			if fileCount != 0 {
				metadata += ","
			}
			metadata += file
			fileCount++
		}
		mCount++
	}
	fs.lock.Unlock()
	// fmt.Println(metadata)

	// send the string
	client, err := getRPCClient(machineIP)
	if err != nil {
		checkError(err)
		// return errors.New("cannot connect with machine " + machineIP)
	}
	var reply int
	args := Args{"", metadata, ""}
	err = client.Call("FileSystemService.HandleFileMetaData", args, &reply)
	if err != nil {
		fmt.Printf("%v", err)
		client.Close()
		return errors.New("machine " + machine + " failed to receive metadata")
	}
	client.Close()

	return nil
}

// Contains checks if a file is contained in SDFS
func (fs *FileSystemService) Contains(file string) bool {
	_, ok := fs.ft.file2timestamp[file]
	return ok
}

// ListAllFiles lists all files stored in SDFS
func (fs *FileSystemService) ListAllFiles() []string {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	keys := make([]string, len(fs.ft.file2machines))
	i := 0
	for k := range fs.ft.file2machines {
		keys[i] = k
		i++
	}

	return keys
}
