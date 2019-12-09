package maplejuice

import (
	"container/list"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	F "../filesystem"
)

// MasterService provides services of MapleJuice's master
type MasterService struct {
	fs              *F.FileSystemService
	currJob         *Job
	jobQueue        *list.List
	taskMap         map[string]*list.List
	activeWorkers   map[string]int
	inactiveWorkers map[string]int
	juiceResult     sync.Map
	jobQueueLock    sync.Mutex
	taskMapLock     sync.Mutex
	workerListLock  sync.Mutex
	wg              sync.WaitGroup
}

// StartService starts master service of MapleJuice
func (master *MasterService) StartService(fs *F.FileSystemService) error {
	// initialize MasterService
	fs.IsMaster = true
	master.fs = fs
	master.currJob = nil
	master.jobQueue = list.New()
	master.taskMap = make(map[string]*list.List)
	master.activeWorkers = make(map[string]int)
	master.inactiveWorkers = make(map[string]int)
	master.juiceResult = sync.Map{}
	selfIP := getIPfromID(master.fs.Ms.ListSelf())
	for _, memberID := range master.fs.Ms.GetMembershipList() {
		if memberIP := getIPfromID(memberID); memberIP != selfIP {
			master.inactiveWorkers[memberIP] = 1
		}
	}

	// setup RPC service
	rpc.Register(master)
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

	// start membership update monitors
	go master.monitorJoin()
	go master.monitorLeave()
	go master.monitorFailure()

	// start job processing
	go master.processJob()

	fmt.Println("[master] started")
	return nil
}

// SubmitMaple submits a Maple job
func (master *MasterService) SubmitMaple(strList []string) error {
	log.Println("[MapleJuice] submitted maple job")

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

	var reply int
	err = master.HandleMapleRequest(job, &reply)
	if err != nil {
		return errors.New("submit maple request failed")
	}

	fmt.Println("[INFO] job submitted!")
	return nil
}

// SubmitJuice submits a Juice job
func (master *MasterService) SubmitJuice(strList []string) error {
	log.Println("[MapleJuice] submited juice job")

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

	var reply int
	err = master.HandleJuiceRequest(job, &reply)
	if err != nil {
		return errors.New("submit juice request failed")
	}

	fmt.Println("[INFO] job submitted!")
	return nil
}

// HandleMapleRequest handles requests of Maple tasks
func (master *MasterService) HandleMapleRequest(job *Job, reply *int) error {
	log.Println("[MapleJuice] received maple job request")
	master.jobQueueLock.Lock()
	master.jobQueue.PushBack(job)
	master.jobQueueLock.Unlock()
	return nil
}

// HandleJuiceRequest handles requests of Juice tasks
func (master *MasterService) HandleJuiceRequest(job *Job, reply *int) error {
	log.Println("[MapleJuice] received juice job request")
	master.jobQueueLock.Lock()
	master.jobQueue.PushBack(job)
	master.jobQueueLock.Unlock()
	return nil
}

// HandleMapleResult gathers results from Maple tasks
func (master *MasterService) HandleMapleResult(result *JobResult, reply *int) error {
	log.Println("[MapleJuice] received maple result")
	job := master.currJob

	for i, key := range result.Keys {
		filename := job.IntermediateFilePrefix + key

		// append result to file
		// If the O_APPEND flag of the file status flags is set,
		//   the file offset shall be set to the end of the file
		//  prior to each write and no intervening file modification
		//  operation shall occur between changing the file offset
		//  and the write operation.
		file, err := os.OpenFile("temp/"+filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			checkError(err)
			file.Close()
			return err
		}
		_, err = file.WriteString(result.Results[i] + "\n")
		if err != nil {
			checkError(err)
			file.Close()
			return err
		}
		file.Close()
	}

	log.Println("[MapleJuice] maple result processed")
	return nil
}

// HandleJuiceResult gathers results from Juice tasks
func (master *MasterService) HandleJuiceResult(result *JobResult, reply *int) error {
	log.Println("[MapleJuice] received juice result")

	for i, k := range result.Keys {
		master.juiceResult.Store(k, result.Results[i])
	}

	log.Println("[MapleJuice] juice result processed")
	return nil
}

// processJob processes jobs in the queue one by one
func (master *MasterService) processJob() {
	for {
		master.jobQueueLock.Lock()
		queueLen := master.jobQueue.Len()
		master.jobQueueLock.Unlock()
		if queueLen > 0 {
			fmt.Println("[MapleJuice] job processing started")
			log.Println("[MapleJuice] job processing started")
			start := time.Now()

			// delete existing temp folder and create a new one for storing temp files
			os.RemoveAll("temp")
			os.Mkdir("temp", 0777)

			// get the job at the front of the queue
			master.jobQueueLock.Lock()
			master.currJob = master.jobQueue.Front().Value.(*Job)
			master.jobQueueLock.Unlock()

			// dispatch task
			if master.currJob.Type == 0 {
				master.dispatchMapleTask(master.currJob)
			} else {
				master.dispatchJuiceTask(master.currJob)
			}

			// dequeue the processesed job
			master.jobQueueLock.Lock()
			master.jobQueue.Remove(master.jobQueue.Front())
			master.jobQueueLock.Unlock()

			// reset taskMap
			master.taskMapLock.Lock()
			master.taskMap = make(map[string]*list.List)
			master.taskMapLock.Unlock()

			elapsed := time.Since(start)
			fmt.Printf("Execution time: %.1f s\n", elapsed.Seconds())
			log.Println("[MapleJuice] job processing finished")
			fmt.Println("[MapleJuice] job processing finished")
		}
		runtime.Gosched()
	}
}

// dispatchMapleTask dispatches maple tasks to workers
func (master *MasterService) dispatchMapleTask(job *Job) error {
	// partition input files
	srcFilePartition := master.partitionFiles(job.NumWorkers, job.SrcDirectory)

	// determine worker
	workers := master.selectWorkers(job.NumWorkers, srcFilePartition)
	log.Printf("[MapleJuice] assigned workers: %v", workers)

	// dispatch tasks
	master.wg.Add(len(workers))
	for i, worker := range workers {
		task := new(Job)
		*task = *job
		task.SrcDirectory = srcFilePartition[i]
		master.taskMapLock.Lock()
		master.taskMap[worker].PushBack(task)
		master.taskMapLock.Unlock()

		// failure will be handled by failure recovery
		go master.dispatchTask(task, worker)
	}
	master.wg.Wait()

	// add intermediate files to SDFS
	pattern := "temp/" + job.IntermediateFilePrefix + "*"
	matches, err := filepath.Glob(pattern)
	checkError(err)
	for _, match := range matches {
		master.fs.Put(match, match[5:])
	}

	return nil
}

// dispatchJuiceTask dispatches juice tasks to workers
func (master *MasterService) dispatchJuiceTask(job *Job) error {
	// if NumWorkers more than number of keys, cut it down
	var matches []string
	fileList := master.fs.ListAllFiles()
	for _, file := range fileList {
		if strings.HasPrefix(file, job.IntermediateFilePrefix) {
			matches = append(matches, file)
		}
	}
	sort.Strings(matches) // matches is sorted
	if len(matches) < job.NumWorkers {
		job.NumWorkers = len(matches)
		log.Println("[MapleJuice] <Juice> NumWorkers more than number of keys, cut it down")
	}

	// shuffle and group
	var srcFilePartition [][]string
	switch job.Partitioner {
	case "range":
		log.Println("[MapleJuice] <Juice> use range partitioner")
		srcFilePartition = rangePartition(matches, job.NumWorkers)
	case "hash":
		log.Println("[MapleJuice] <Juice> use hash partitioner")
		srcFilePartition = hashPartition(matches, job.NumWorkers)
	default:
		return errors.New("partitioner type not recognized, only supports range/hash")
	}

	// determine workers
	workers := master.selectWorkers(job.NumWorkers, srcFilePartition)
	log.Printf("[MapleJuice] assigned workers: %v", workers)

	// dispatch tasks
	master.wg.Add(len(workers))
	for i, worker := range workers {
		task := new(Job)
		*task = *job
		task.SrcDirectory = srcFilePartition[i]
		master.taskMapLock.Lock()
		master.taskMap[worker].PushBack(task)
		master.taskMapLock.Unlock()

		// failure will be handled by failure recovery
		go master.dispatchTask(task, worker)
	}
	master.wg.Wait()

	// write to dest file
	file, err := os.OpenFile("temp/"+job.DestFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	defer file.Close()
	if err != nil {
		return err
	}
	prefixLen := len(job.IntermediateFilePrefix)
	for _, match := range matches {
		key := match[prefixLen:]
		content, _ := master.juiceResult.Load(key)
		l, err := file.WriteString(key + "\t" + content.(string))
		if err != nil {
			return err
		}
		log.Println("[MapleJuice]", l, "bytes written successfully")
	}

	// add dest file to SDFS
	master.fs.Put("temp/"+job.DestFile, job.DestFile)

	// delete input if specified
	if job.DeleteInput {
		for _, file := range matches {
			err := master.fs.Delete(file)
			checkError(err)
		}
		log.Println("[MapleJuice] <Juice> input deleted")
	}

	return nil
}

// partitionFiles partitions input files for Maple
func (master *MasterService) partitionFiles(numWorkers int, srcFiles []string) [][]string {
	srcFilePartition := make([][]string, numWorkers)

	if len(srcFiles) >= numWorkers {
		for i, file := range srcFiles {
			idx := i % numWorkers
			srcFilePartition[idx] = append(srcFilePartition[idx], file)
		}
	} else {
		// concatenate files into one single file
		filePath := "sdfs/"
		for _, file := range srcFiles {
			if !master.fs.Contains(file) {
				log.Printf("[master] doesn't have file %s, call GET", file)
				master.fs.Get(file, "temp/"+file)
				filePath = "temp/"
			}
			command := "cat " + filePath + file + " > temp/__tmp_maple_input"
			cmd := exec.Command("bash", "-c", command)
			cmd.Run()
		}

		// get the line count of that file
		command := "wc -l temp/__tmp_maple_input"
		out, err := exec.Command("bash", "-c", command).Output()
		checkError(err)
		lineCount, err := strconv.Atoi(strings.Fields(string(out))[0])
		checkError(err)
		log.Println("[MapleJuice] <MaplePartitionFiles> total line count:", lineCount)

		// determine lines of each splitted file
		cnt := int(math.Ceil(float64(lineCount / numWorkers)))

		// split into files
		command = "split -l " + string(cnt) + "__tmp_maple_input __tmp"
		cmd := exec.Command("bash", "-c", command)
		cmd.Run()

		// add files to SDFS
		pattern := "temp/__tmp*"
		matches, err := filepath.Glob(pattern)
		checkError(err)
		for i, match := range matches {
			name := match[5:]
			master.fs.Put(match, name)
			srcFilePartition[i] = append(srcFilePartition[i], name)
		}
	}

	return srcFilePartition
}

// selectWorkers selects workers to dispatch tasks to
func (master *MasterService) selectWorkers(numWorkers int, srcFilePartition [][]string) []string {
	master.workerListLock.Lock()
	defer master.workerListLock.Unlock()

	// determine worker
	workers := make([]string, numWorkers)
	for i := 0; i < numWorkers; i++ {
		file := srcFilePartition[i][0]
		machines, _ := master.fs.ListMachines(file)
		selected := false
		// #1 preference: free workers with files
		for _, machine := range machines {
			if _, ok := master.inactiveWorkers[machine]; ok {
				workers[i] = machine
				master.activeWorkers[machine] = 1
				delete(master.inactiveWorkers, machine)
				master.taskMapLock.Lock()
				master.taskMap[machine] = list.New()
				master.taskMapLock.Unlock()
				selected = true
				break
			}
		}
		// #2 preference: free workers without files
		if !selected {
			for machine := range master.inactiveWorkers {
				workers[i] = machine
				master.activeWorkers[machine] = 1
				delete(master.inactiveWorkers, machine)
				master.taskMapLock.Lock()
				master.taskMap[machine] = list.New()
				master.taskMapLock.Unlock()
				selected = true
				break
			}
		}
		// #3 preference: any other running workers
		if !selected {
			for machine := range master.activeWorkers {
				workers[i] = machine
				selected = true
				break
			}
		}
	}

	return workers
}

// dispatchTask dispatches task to selected worker
func (master *MasterService) dispatchTask(task *Job, worker string) error {
	var taskType string
	if task.Type == 0 {
		taskType = "RunMapleTask"
	} else {
		taskType = "RunJuiceTask"
	}

	client, err := getRPCClient(worker)
	if err != nil {
		log.Printf("[MapleJuice] <dispatchTask> dialing %s to getRPCClient failed: %v\n", worker, err)
		return err
	}

	var response int
	err = client.Call("WorkerService."+taskType, task, &response)
	if err != nil {
		log.Printf("[MapleJuice] <dispatchTask> RPC to %s on %s failed\n", taskType, worker)
		client.Close()
		return err
	}
	client.Close()

	// update taskMap
	master.taskMapLock.Lock()
	master.taskMap[worker].Remove(master.taskMap[worker].Front())
	master.taskMapLock.Unlock()

	// update activeWorkers and inactiveWorkers
	master.workerListLock.Lock()
	delete(master.activeWorkers, worker)
	master.inactiveWorkers[worker] = 1
	master.workerListLock.Unlock()

	// mark as finished only when job is successfuly finished
	// if failed, failure recovery will handle that
	master.wg.Done()
	fmt.Println("one task finished")

	return nil
}

// handleWorkerFailure deals with the case where workers fail or leave
func (master *MasterService) handleWorkerFailure(failedMachine string) {
	master.workerListLock.Lock()
	// if inactive worker failed, delete entry and we are done
	if _, ok := master.activeWorkers[failedMachine]; !ok {
		delete(master.inactiveWorkers, failedMachine)
		master.workerListLock.Unlock()
		log.Println("[MapleJuice] <Failure Recovery> failed worker is inactive, no need for failure recovery")
		return
	}

	// otherwise, start re-dispatching task to a new worker
	log.Println("[MapleJuice] <Failure Recovery> failed worker is active, start failure recovery")
	delete(master.activeWorkers, failedMachine)

	// select a new worker
	var worker string
	if len(master.inactiveWorkers) > 0 { // prefer free workers
		for machine := range master.inactiveWorkers {
			worker = machine
			master.activeWorkers[machine] = 1
			delete(master.inactiveWorkers, machine)
			master.taskMapLock.Lock()
			master.taskMap[machine] = list.New()
			master.taskMapLock.Unlock()
			break
		}
	} else {
		for machine := range master.activeWorkers {
			worker = machine
			break
		}
	}
	log.Printf("[MapleJuice] <Failure Recovery> select %s to reprocess the task", worker)
	master.workerListLock.Unlock()

	// retrieve unfinished task(s) and assign it to the new worker
	master.taskMapLock.Lock()
	taskList := master.taskMap[failedMachine]
	master.taskMap[worker].PushBackList(taskList)
	master.taskMapLock.Unlock()

	for taskList.Len() > 0 {
		task := taskList.Front().Value.(*Job)

		// failure will be handled by failure recovery
		go master.dispatchTask(task, worker)

		taskList.Remove(taskList.Front())
	}
	log.Println("[MapleJuice] <Failure Recovery> failure recovery finished")
}

// monitorJoin monitors if a new node has joined the distributed system
func (master *MasterService) monitorJoin() {
	for {
		if machineID, ok := <-master.fs.JoinReportChan; ok {
			machine := getIPfromID(machineID)
			log.Println("[MapleJuice] monitored join of machine ", machine)
			master.workerListLock.Lock()
			master.inactiveWorkers[machine] = 1
			master.workerListLock.Unlock()
		}
		runtime.Gosched()
	}
}

// monitorLeave monitors leaves of group members
func (master *MasterService) monitorLeave() {
	for {
		if machineID, ok := <-master.fs.LeaveReportChan; ok {
			machine := getIPfromID(machineID)
			log.Println("[MapleJuice] monitored leave of machine ", machine)
			go master.handleWorkerFailure(machine)
		}
		runtime.Gosched()
	}
}

// monitorFailure monitors failures of group members
func (master *MasterService) monitorFailure() {
	for {
		if machineID, ok := <-master.fs.FailReportChan; ok {
			machine := getIPfromID(machineID)
			log.Println("[MapleJuice] monitored failure of machine ", machine)
			go master.handleWorkerFailure(machine)
		}
		runtime.Gosched()
	}
}
