package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	F "./filesystem"
	MapleJuice "./maplejuice"
	M "./membership"
)

func main() {
	// command line interface
	if len(os.Args) < 2 || len(os.Args) > 3 || (len(os.Args) == 3 && os.Args[2] != "--log2screen") {
		fmt.Println("Usage:", os.Args[0], "<port_number>")
		fmt.Println("Log to file:", os.Args[0], "<port_number> --log2screen")
		os.Exit(1)
	}
	portNum := os.Args[1]
	lossRate := 0
	if len(os.Args) == 2 {
		os.Remove("vm-log.txt")
		f, err := os.OpenFile("vm-log.txt", os.O_RDWR|os.O_CREATE, 0666)
		checkFatalError(err)
		defer f.Close()
		log.SetOutput(f)
	}

	// start membership service
	ms := new(M.MembershipService)
	ms.StartService(portNum, lossRate)

	// start filesyetem service on top of the membership service
	fs := new(F.FileSystemService)
	err := fs.StartService(ms)
	checkFatalError(err)

	// prepare maplejuice service, which is on top of the filesystem service
	var master MapleJuice.MasterService
	var worker MapleJuice.WorkerService

	// auxiliary bool variables
	responded := false      // for time-bounded write conflict of SDFS
	timeoutStarted := false // for time-bounded write conflict of SDFS
	isMaster := false       // am I master of MapleJuice

	// parsing and running input commands
	var s string
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		// read input
		s = scanner.Text()
		strList := strings.Fields(s)
		if len(strList) < 1 {
			continue
		}

		// parse input and dispatch handlers
		switch strList[0] {

		/* Membership commands */
		case "join":
			if len(strList) == 1 {
				ms.JoinGroup()
			} else {
				fmt.Println("[Usage] join")
			}
		case "leave":
			if len(strList) == 1 {
				ms.LeaveGroup()
				os.Exit(0)
			} else {
				fmt.Println("[Usage] leave")
			}
		case "id":
			if len(strList) == 1 {
				self := ms.ListSelf()
				fmt.Println("[machine] id: " + self)
			} else {
				fmt.Println("[Usage] id")
			}
		case "ml":
			if len(strList) == 1 {
				memberList := ms.ListMembers()
				for _, member := range memberList {
					fmt.Println(member)
				}
			} else {
				fmt.Println("[Usage] ml")
			}
		case "introducer":
			if len(strList) == 1 {
				go ms.StartIntroducer()
			} else {
				fmt.Println("[Usage] introducer")
			}

		/* SDFS commands */
		case "put":
			if len(strList) == 3 {
				err := fs.PrePut(strList[2])
				if err != nil {
					log.Printf("error: %v", err)
					fmt.Println("Someone is updating the same file. Do you really want to update now? (yes/no)")
					timeoutStarted = true
					go responseTimeout(&responded, &timeoutStarted)
					for scanner.Scan() {
						s = scanner.Text()
						if timeoutStarted {
							if s == "yes" {
								responded = true
								err := fs.Put(strList[1], strList[2])
								checkError(err)
								fmt.Println("Write forced.")
							} else if s == "no" {
								responded = true
								fmt.Println("Write cancelled.")
							} else {
								fmt.Println("Answer yes or no")
							}
							continue
						} else {
							break
						}
					}
				} else {
					err := fs.Put(strList[1], strList[2])
					checkError(err)
				}
			} else {
				fmt.Println("[Usage] put localfilename sdfsfilename")
			}
		case "get":
			if len(strList) == 3 {
				err := fs.Get(strList[1], strList[2])
				checkError(err)
			} else {
				fmt.Println("[Usage] get sdfsfilename localfilename")
			}
		case "delete":
			if len(strList) == 2 {
				err := fs.Delete(strList[1])
				checkError(err)
			} else {
				fmt.Println("[Usage] delete sdfsfilename")
			}
		case "ls":
			if len(strList) == 2 {
				machineList, err := fs.ListMachines(strList[1])
				checkError(err)
				for _, machine := range machineList {
					fmt.Println(machine)
				}
			} else {
				fmt.Println("[Usage] ls hdfsfilename")
			}
		case "store":
			if len(strList) == 1 {
				fileList, err := fs.ListFiles()
				checkError(err)
				for _, file := range fileList {
					fmt.Println(file)
				}
			} else {
				fmt.Println("[Usage] store")
			}
		case "allfile":
			if len(strList) == 1 {
				fileList := fs.ListAllFiles()
				for _, file := range fileList {
					fmt.Println(file)
				}
			}

		/* MapleJuice commands */
		case "master":
			if len(strList) == 1 {
				err := master.StartService(fs)
				checkFatalError(err)
				isMaster = true
			} else {
				fmt.Println("[Usage] master")
			}
		case "worker":
			if len(strList) == 1 {
				err := worker.StartService(fs)
				checkFatalError(err)
			} else {
				fmt.Println("[Usage] worker")
			}
		case "maple":
			if len(strList) >= 5 {
				if isMaster {
					err := master.SubmitMaple(strList)
					checkError(err)
				} else {
					err := worker.SubmitMaple(strList)
					checkError(err)
				}
			} else {
				fmt.Println("[Usage] maple <maple_exe> <num_maples>")
				fmt.Println("        <sdfs_intermediate_filename_prefix> <sdfs_src_files>")
			}
		case "juice":
			if len(strList) == 6 || len(strList) == 7 {
				if isMaster {
					err := master.SubmitJuice(strList)
					checkError(err)
				} else {
					err := worker.SubmitJuice(strList)
					checkError(err)
				}
			} else {
				fmt.Println("[Usage] juice <juice_exe> <num_juices>")
				fmt.Println("        <sdfs_intermediate_filename_prefix> <sdfs_dest_filename>")
				fmt.Println("        <delete_input={0,1}> [partitioner={range, hash}](optional, default=range)")
			}

		default:
			fmt.Println("Unrecogonized command")
		}
	}
}

func responseTimeout(responded *bool, timeoutStarted *bool) {
	startTime := time.Now()
	ddl := startTime.Add(time.Duration(30 * time.Second))
	for time.Now().Before(ddl) {
		if *responded {
			*timeoutStarted = false
			return
		}
	}
	*timeoutStarted = false
	fmt.Println("Response timeout, write cancelled.")
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
