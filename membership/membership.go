package membership

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	tset "github.com/emirpasic/gods/sets/treeset"
)

// Type of messages
const (
	JOINGOSSIP int = 2
	LEAVE      int = 3
	FAIL       int = 4
	HEARTBEAT  int = 5
	MASTER     int = 6
	ELECTION   int = 7
	OK         int = 8
)

type MembershipList struct {
	members *tset.Set
	lock    sync.Mutex
}

type MembershipService struct {
	Successors      []string    // 4 successors
	Predecessors    []string    // 3 predecessors
	LeaveReportChan chan string // leave report channel
	FailReportChan  chan string // failure report channel
	JoinReportChan  chan string // join report channel

	msl            MembershipList
	conn           *net.UDPConn
	time           string
	timer          map[string]time.Time
	lossRate       int
	pingCounter    int
	failCounter    int
	neighbors_lock sync.Mutex
	timer_lock     sync.Mutex
	master         string
	receivedOK     bool
}

// StartService starts membership service
func (ms *MembershipService) StartService(portNum string, lossRate int) {
	rand.Seed(time.Now().UnixNano())
	service := ":" + portNum

	// create UDP address
	addr, err := net.ResolveUDPAddr("udp", service)
	if err != nil {
		log.Fatalf("error resolve UDP addr: %v", err)
	}

	// create UDP listener
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("error create UDP listener: %v", err)
	}

	// initialize variables of membership services
	ms.msl.members = tset.NewWithStringComparator()
	ms.conn = conn
	ms.timer = make(map[string]time.Time)
	// ms.neighbors = make(map[string]time.Time)
	ms.lossRate = lossRate
	ms.LeaveReportChan = make(chan string)
	ms.JoinReportChan = make(chan string)
	ms.FailReportChan = make(chan string)
}

// StartIntroducer starts introducer service
func (ms *MembershipService) StartIntroducer() {
	err := ioutil.WriteFile("./introducer-log.txt", []byte(""), 0644)
	service := ":1234"

	// create UDP address
	udpAddr, err := net.ResolveUDPAddr("udp", service)
	checkError(err)

	// create UDP listener
	intro, err := net.ListenUDP("udp", udpAddr)
	checkError(err)

	// initialize membership map
	fmt.Println("[introducer] started")
	for {
		// log.Printf("start new handleJoin")
		ms.handleJoin(intro)
		// log.Printf("end one handleJoin")
	}
}

func (ms *MembershipService) JoinGroup() {
	content, err := ioutil.ReadFile("introducer.config")
	if err != nil {
		log.Fatalf("cannot open introducer.config: %v", err)
	}
	services := strings.Split(string(content), "\n")

	for i := range services {
		err = ms.dialIntroducer(services[i])
		if err != nil {
			// log.Printf("%s not responding", services[i])
			time.Sleep(500 * time.Millisecond)
			continue
		} else {
			masterArr := strings.Split(services[i], ":")
			ms.master = masterArr[0]
			break
		}
	}

	// log.Println("Joined group")

	// start listening to messages
	go ms.listenMessage()

	// start heartbeating
	go ms.heartbeat()

	// start timeout checking
	go ms.checker()
}

func (ms *MembershipService) LeaveGroup() {
	msg := createMessage(LEAVE, ms.ListSelf())
	ms.disseminateMessage(msg)
	// log.Printf("failed: %d, Ping %d", ms.failCounter, ms.pingCounter)
	// log.Println("Left group")
}

func (ms *MembershipService) ListMembers() []string {
	ms.msl.lock.Lock()
	res := ms.msl.members.Values()
	ms.msl.lock.Unlock()
	members := make([]string, len(res))
	for i, v := range res {
		members[i] = fmt.Sprint(v)
	}
	return members
}

func (ms *MembershipService) ListSelf() string {
	s := ms.conn.LocalAddr().String()
	return getLocalIP() + s[len(s)-5:] + "|" + ms.time
}

func (ms *MembershipService) listenMessage() {
	// log.Println("Start listening to messages")

	var buf [1024]byte
	for {
		ms.conn.SetReadDeadline(time.Now().Add(1 * time.Hour))
		n, _, err := ms.conn.ReadFromUDP(buf[0:])
		if err != nil {
			log.Fatal("cannot read from UDP: %v", err)
		}
		msg := decodeMessage(string(buf[0:n]))
		// log.Printf("received message type: %d", msg.tag)
		switch msg.tag {
		case JOINGOSSIP:
			go ms.handleJoinGossip(msg)
		case LEAVE:
			go ms.handleLeave(msg)
		case FAIL:
			go ms.handleFail(msg)
		case HEARTBEAT:
			go ms.handleHeartbeat(msg.content)
		case MASTER:
			go ms.handleMaster(msg.content)
		case ELECTION:
			go ms.handleElection(msg.content)
		case OK:
			go ms.handleOK()
		}
	}
}

func (ms *MembershipService) heartbeat() {
	// log.Println("Start Heartbeating")
	msg := createMessage(HEARTBEAT, ms.ListSelf())
	for {
		ms.neighbors_lock.Lock()
		for i, successor := range ms.Successors {
			// log.Println("Heartbeat neighbor " + successor)
			successorStrs := strings.Split(successor, "|")
			addr, err := net.ResolveUDPAddr("udp", successorStrs[0])
			checkError(err)
			ms.sendMessage(msg, addr)
			ms.pingCounter++

			// only heartbeating 3 neighbors
			if i == 3 {
				break
			}
		}
		ms.neighbors_lock.Unlock()
		time.Sleep(1 * time.Second)
	}
}

func (ms *MembershipService) disseminateMessage(msg message) {
	// log.Printf("start disseminate %s", encodeMessage(msg))
	ms.neighbors_lock.Lock()
	for _, successor := range ms.Successors {
		successorStrs := strings.Split(successor, "|")
		udpAddr, err := net.ResolveUDPAddr("udp", successorStrs[0])
		checkError(err)
		ms.sendMessage(msg, udpAddr)
	}
	ms.neighbors_lock.Unlock()
}

func (ms *MembershipService) updateNeighbors() {
	// log.Printf("MembershipList updated: %v", ms.ListMembers())
	ms.neighbors_lock.Lock()
	ms.timer_lock.Lock()
	ms.Successors = ms.msl.getSuccessors(ms.ListSelf(), 4)     // 4 for message dissemination
	ms.Predecessors = ms.msl.getPredecessors(ms.ListSelf(), 3) // 3 for failure detection
	// log.Printf("Successors updated: %v", ms.Successors)
	// log.Printf("Predecessors updated: %v", ms.Predecessors)
	for _, pre := range ms.Predecessors {
		if _, ok := ms.timer[pre]; ok {
			continue
		} else {
			// if new predecessor have not been included
			// set the new predecessor timer as now
			ms.timer[pre] = time.Now()
		}
	}
	ms.timer_lock.Unlock()
	ms.neighbors_lock.Unlock()
}

func (ms *MembershipService) handleJoin(conn *net.UDPConn) {
	/*
		introducer handles join request
	*/
	var buf [1024]byte
	n, addr, err := conn.ReadFromUDP(buf[0:])
	fmt.Println("[introducer] received from: " + addr.String())
	if err != nil {
		log.Fatal(err)
	}

	t := string(buf[0:n])
	id := addr.String() + "|" + t
	ms.msl.add(id)
	// log.Println("[introducer] received id: " + id)
	ms.JoinReportChan <- id

	// write to log
	// daytime := time.Now().String()
	// log.Println("[" + daytime + "] ID: " + addr.String() + "|" + string(buf[0:n]))

	// compress membershit list into one string
	it := ms.msl.members.Iterator()
	var memberWrap []string
	for it.Next() {
		if str, ok := it.Value().(string); ok {
			memberWrap = append(memberWrap, str)
		}
	}
	memberWrapStr := strings.Join(memberWrap, ",")

	conn.WriteToUDP([]byte(memberWrapStr), addr)

	ms.updateNeighbors()
	ms.disseminateMessage(createMessage(JOINGOSSIP, id))
}

func (ms *MembershipService) dialIntroducer(service string) error {
	udpAddr, err := net.ResolveUDPAddr("udp", service)
	checkError(err)

	t1 := time.Now()
	t1_str := t1.Format(time.RFC3339)
	ms.time = t1_str
	fmt.Println("[machine] local time: " + t1_str)
	ms.conn.WriteToUDP([]byte(t1_str), udpAddr)
	checkError(err)

	var buf [1024]byte
	ms.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, _, err := ms.conn.ReadFromUDP(buf[0:])
	if err != nil {
		return err
	}

	memberStr := strings.Split(string(buf[0:n]), ",")
	for _, m := range memberStr {
		ms.msl.add(m)
	}
	// log.Println("Updated neighbors in dial introducer")
	ms.updateNeighbors()
	return nil
}

func (ms *MembershipService) checker() {
	for {
		time.Sleep(200 * time.Millisecond)
		var failedProcesses []string

		ms.timer_lock.Lock()
		for k, v := range ms.timer {
			found := false

			ms.neighbors_lock.Lock()
			for _, n := range ms.Predecessors {
				if k == n {
					found = true
					break
				}
			}
			ms.neighbors_lock.Unlock()

			if found == false {
				delete(ms.timer, k)
				continue
			}
			ddl := v.Add(time.Duration(4 * time.Second))
			if ddl.Before(time.Now()) {
				// failure detected

				// log.Printf("last HB received time: %v", v)
				// log.Printf(k + " detected as Failed")

				failedProcesses = append(failedProcesses, k)
				ms.msl.remove(k)
				ms.FailReportChan <- k // report failure

				kSplit := strings.Split(k, ":")
				if kSplit[0] == ms.master {
					// log.Printf("detected master %s failed", kSplit[0])
					ms.timer_lock.Unlock()
					ms.electNewMaster()
					ms.timer_lock.Lock()
				}

				failMsg := createMessage(FAIL, k)
				ms.disseminateMessage(failMsg)
			}
		}
		for _, k := range failedProcesses {
			delete(ms.timer, k)
		}
		ms.timer_lock.Unlock()

		if len(failedProcesses) > 0 {
			ms.updateNeighbors()
		}
	}
}

func (ms *MembershipService) electNewMaster() {
	// check if the I have the biggest IP address
	failedMaster := ms.master
	ms.msl.lock.Lock()
	// myIP := ms.ListSelf()
	it := ms.msl.members.Iterator()
	it.Last()
	// log.Printf("membership list biggest IP: %s", it.Value().(string))
	// log.Printf("my ip: %s", ms.ListSelf())
	if (it.Value().(string)) == ms.ListSelf() {
		// if I have the biggest IP, then I am the new master (coordinator)
		ms.becomeMaster()
	} else {
		// send ELECTION msg to all machines with bigger IP
		for it.Value().(string) != ms.ListSelf() {
			go ms.sendElection(it.Value().(string))
			if !it.Next() {
				break
			}
			// if (ms.master != failedMaster || !it.Prev() || ms.receivedOK) {
			// 	break
			// }
			// // log.Printf("ELECTION to %s timeout", ipArr[0])
		}
		ms.msl.lock.Unlock()
		// TO DISCUSS: timeout by sleeping
		time.Sleep(2 * time.Second)
		// claim to be the master if master hasn't been updated and OK msg is never received
		if ms.master == failedMaster && !ms.receivedOK {
			ms.becomeMaster()
		}
	}
	return
}

func (ms *MembershipService) becomeMaster() {
	// log.Printf("claim to be the master")
	myIP := ms.ListSelf()
	ipArr := strings.Split(myIP, ":")
	ms.master = ipArr[0]
	go ms.StartIntroducer()
	msg := createMessage(MASTER, ipArr[0])
	// broadcast
	// log.Println("start to broadcast MASTER")
	it := ms.msl.members.Iterator()
	for it.Next() {
		if it.Value().(string) != ms.ListSelf() {
			ipArr := strings.Split(it.Value().(string), "|")
			udpAddr, err := net.ResolveUDPAddr("udp", ipArr[0])
			checkError(err)
			// log.Printf("send MASTER msg to %s", ipArr[0])
			ms.sendMessage(msg, udpAddr)
		}
	}
}

func (ms *MembershipService) sendElection(receiverIP string) {
	ipArr := strings.Split(receiverIP, "|")
	udpAddr, err := net.ResolveUDPAddr("udp", ipArr[0])
	checkError(err)
	msg := createMessage(ELECTION, ms.ListSelf())
	// log.Printf("send ELECTION to %s", ipArr[0])
	ms.sendMessage(msg, udpAddr)
}

func (ms *MembershipService) checkIfMaster() bool {
	// check if I am the master
	return strings.HasPrefix(ms.ListSelf(), ms.master)
}

func (ms *MembershipService) GetMembershipList() []string {
	var membershipList []string
	ms.msl.lock.Lock()
	it := ms.msl.members.Iterator()
	for it.Next() {
		membershipList = append(membershipList, it.Value().(string))
	}
	ms.msl.lock.Unlock()
	return membershipList
}

func getLocalIP() string {
	host, _ := os.Hostname()
	addrs, _ := net.LookupIP(host)
	addr := addrs[0]
	ipv4 := addr.To4()
	return ipv4.String()
}

func checkError(err error) {
	if err != nil {
		log.Fatalf("Fatal error: %v", err)
	}
}

func getIPfromID(id string) string {
	idArr := strings.Split(id, ":")
	return idArr[0]
}
