package filesystem

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

// HandleInsert handles a remote insert file request
func (fs *FileSystemService) HandleInsert(args Args, reply *string) error {
	log.Println("Received HandleInsert")

	sdfsfilename := args.Filename
	content := args.Content
	timestamp := args.Timestamp

	file, err := os.OpenFile("sdfs/"+sdfsfilename, os.O_RDWR|os.O_CREATE, 0777)
	defer file.Close()
	if err != nil {
		return err
	}
	l, err := file.WriteString(content)
	if err != nil {
		return err
	}
	log.Println(l, "bytes written successfully")

	// update file table
	selfIP := getIPfromID(fs.Ms.ListSelf())
	fs.lock.Lock()
	fs.ft.file2timestamp[sdfsfilename] = timestamp
	if fs.ft.file2machines[sdfsfilename] == nil {
		fs.ft.file2machines[sdfsfilename] = make(map[string]int)
	}
	fs.ft.file2machines[sdfsfilename][selfIP] = 1
	if fs.ft.machine2files[selfIP] == nil {
		fs.ft.machine2files[selfIP] = make(map[string]int)
	}
	fs.ft.machine2files[selfIP][sdfsfilename] = 1
	fs.lock.Unlock()

	// broadcast that I have received a new file
	msl := fs.Ms.GetMembershipList()

	for _, memberID := range msl {
		memberIP := getIPfromID(memberID)
		if memberIP == selfIP {
			continue
		}
		client, err := getRPCClient(memberIP)
		log.Println("Send HandleInsert Broadcast to ", memberIP)
		if err != nil {
			log.Println("cannot connect with member", memberIP)
			continue
		}
		var reply int
		args := Args{sdfsfilename, selfIP, timestamp}
		err = client.Call("FileSystemService.HandleInsertBroadcast", args, &reply)
		if err != nil {
			log.Println("member", memberIP, "failed to receive broadcast")
		}
		client.Close()
	}
	return nil
}

// HandleInsertBroadcast handles a remote broadcast of file inserted info request
func (fs *FileSystemService) HandleInsertBroadcast(args Args, reply *int) error {
	log.Println("Received HandleInsertBroadcast")

	sdfsfilename := args.Filename
	machineIP := args.Content
	fs.lock.Lock()
	if fs.ft.file2machines[sdfsfilename] == nil {
		fs.ft.file2machines[sdfsfilename] = make(map[string]int)
	}
	fs.ft.file2machines[sdfsfilename][machineIP] = 1
	if fs.ft.machine2files[machineIP] == nil {
		fs.ft.machine2files[machineIP] = make(map[string]int)
	}
	fs.ft.machine2files[machineIP][sdfsfilename] = 1
	fs.lock.Unlock()

	return nil
}

// HandleUpdate handles a remote update file request
func (fs *FileSystemService) HandleUpdate(args *Args, reply *int) error {
	log.Println("Received HandleUpdate")

	sdfsfilename := args.Filename
	content := args.Content
	timestamp := args.Timestamp
	fs.lock.Lock()
	if _, ok := fs.ft.file2timestamp[sdfsfilename]; !ok {
		log.Println("file " + sdfsfilename + " not present in filetable")
		return errors.New("file not present upon update")
	}
	fs.ft.file2timestamp[sdfsfilename] = timestamp
	fs.lock.Unlock()

	// actually update file
	err := os.Remove("sdfs/" + sdfsfilename)
	if err != nil {
		fmt.Printf("failed opening file %s\n", sdfsfilename)
		return err
	}

	file, err := os.Create("sdfs/" + sdfsfilename)
	defer file.Close()
	if err != nil {
		return err
	}
	_, err = file.WriteString(content)
	if err != nil {
		return err
	}
	fmt.Printf("%s version updated at %v\n", sdfsfilename, timestamp)

	return nil
}

// HandleGet handles a remote get file request
func (fs *FileSystemService) HandleGet(args *Args, reply *string) error {
	log.Println("Received HandleGet")

	sdfsfilename := args.Filename
	if _, ok := fs.ft.file2timestamp[sdfsfilename]; !ok {
		return errors.New("file " + sdfsfilename + " doesn't exist")
	}

	data, err := ioutil.ReadFile("sdfs/" + sdfsfilename)
	if err != nil {
		log.Printf("file %s doesn't exit", sdfsfilename)
		return err
	}
	// log.Printf("file has content: %s", string(data))
	*reply = string(data)

	return nil
}

// HandleDelete handles a remote delete file request
func (fs *FileSystemService) HandleDelete(args Args, reply *string) error {
	log.Println("Received HandleDelete")

	sdfsfilename := args.Filename

	if _, ok := fs.ft.file2timestamp[sdfsfilename]; ok {
		// delete file
		err := os.Remove("sdfs/" + sdfsfilename)
		if err != nil {
			return err
		}
	}

	// update file table
	fs.lock.Lock()
	delete(fs.ft.file2timestamp, sdfsfilename)
	machines := fs.ft.file2machines[sdfsfilename]
	for machine := range machines {
		delete(fs.ft.machine2files[machine], sdfsfilename)
	}
	delete(fs.ft.file2machines, sdfsfilename)
	fs.lock.Unlock()

	log.Printf("Deleted SDFS file %s", sdfsfilename)
	return nil
}

// HandleRequestToken handles token request for mutual exclusive file writes
func (fs *FileSystemService) HandleRequestToken(args Args, reply *int) error {
	log.Println("Received HandleRequestToken")

	receivedTime := time.Now()
	sdfsfilename := args.Filename

	fs.lock.Lock()
	defer fs.lock.Unlock()
	/*
		if the file is not present in file2token,
			initialize (key, value) as (sdfsfilename, time of reception of write request)
		else
			return file2token[sdfsfilename]

	*/
	if timestamp, ok := fs.ft.file2token[sdfsfilename]; ok {
		ddl := timestamp.Add(time.Duration(1 * time.Minute))
		if ddl.Before(receivedTime) {
			log.Println("last update was more than 1 min ago, grant write request")
			fs.ft.file2token[sdfsfilename] = receivedTime
			*reply = 1
		} else {
			log.Println("last update was less than 1 min ago")
			*reply = 0
		}
	} else {
		fs.ft.file2token[sdfsfilename] = receivedTime
		log.Println("file doesn't exist, grant write request")
		*reply = 1
	}

	return nil
}

// HandleFileMetaData sends meta data of SDFS to newly joined nodes
func (fs *FileSystemService) HandleFileMetaData(args *Args, reply *int) error {
	log.Println("Start to receive file metadata from master")

	metadata := args.Content
	if len(metadata) == 0 {
		log.Println("empty metadata received")
		return nil
	}
	fmt.Println(metadata)
	metadataByMachine := strings.Split(metadata, "|")
	fs.lock.Lock()
	for _, m := range metadataByMachine {
		mArr := strings.Split(m, ":")
		machine := mArr[0]
		files := strings.Split(mArr[1], ",")
		fs.ft.machine2files[machine] = make(map[string]int)
		for _, file := range files {
			if _, ok := fs.ft.file2machines[file]; !ok {
				fs.ft.file2machines[file] = make(map[string]int)
			}
			fs.ft.machine2files[machine][file] = 1
			fs.ft.file2machines[file][machine] = 1
		}
	}
	fs.lock.Unlock()

	return nil
}
