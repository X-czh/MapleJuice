package membership

import (
	"fmt"
	"net"
	"strings"
	"time"
)

func (ms *MembershipService) handleJoinGossip(msg message) {
	id := msg.content
	// log.Printf("Receive info: node %s joined", id)
	if ms.msl.contains(id) == false {
		ms.msl.add(id)
		ms.updateNeighbors()
		ms.disseminateMessage(msg)
	} else {
		// log.Printf("Info(Join) already processed")
	}
}

func (ms *MembershipService) handleLeave(msg message) {
	id := msg.content
	// log.Printf("Receive info: node %s left", id)
	if ms.msl.contains(id) == true {
		ms.msl.remove(id)
		ms.timer_lock.Lock()
		delete(ms.timer, id)
		ms.timer_lock.Unlock()
		ms.updateNeighbors()
		ms.disseminateMessage(msg)
		ms.LeaveReportChan <- id // report leave
	} else {
		// log.Printf("Info(Leave) already processed")
	}
}

func (ms *MembershipService) handleFail(msg message) {
	id := msg.content
	// log.Printf("Receive info: node %s failed", id)
	if ms.msl.contains(id) == true {
		// log.Printf(id + ": Fail")
		ms.msl.remove(id)
		ms.timer_lock.Lock()
		delete(ms.timer, id)
		ms.timer_lock.Unlock()
		ms.updateNeighbors()
		ms.disseminateMessage(msg)
		ms.FailReportChan <- id // report failure
	} else {
		// log.Printf("Info(Fail) already processed")
	}
}

func (ms *MembershipService) handleHeartbeat(neighbor string) {
	t := time.Now()
	ms.timer_lock.Lock()
	if _, ok := ms.timer[neighbor]; ok {
		ms.timer[neighbor] = t
	} else {
		// log.Printf("False Positive detected")
	}
	ms.timer_lock.Unlock()
	// log.Printf("Receive HB from %v at %v", neighbor, t)
}

func (ms *MembershipService) handleMaster(masterIP string) {
	// remove original master from membership list
	// log.Printf("new master: %s", masterIP)
	oldMaster := ms.master
	ms.master = masterIP
	ms.msl.lock.Lock()
	it := ms.msl.members.Iterator()
	// iterate through keys to see if old master is still in membership list
	// if so, delete it
	oldMasterIP := ""
	for it.Next() {
		_, ip := it.Index(), it.Value().(string)
		if strings.HasPrefix(ip, oldMaster) {
			oldMasterIP = ip
			break
		}
	}
	fmt.Println("old master: ", oldMasterIP)
	if oldMasterIP != "" {
		ms.msl.members.Remove(oldMasterIP)
	}

	ms.msl.lock.Unlock()
	ms.receivedOK = false
}

func (ms *MembershipService) handleElection(senderIP string) {
	msg := createMessage(OK, "")
	ipArr := strings.Split(senderIP, "|")
	addr, err := net.ResolveUDPAddr("udp", ipArr[0])
	checkError(err)
	ms.sendMessage(msg, addr)
	// if master failure hasn't been detected, start claiming to be the master
	// log.Printf("current master: %s", ms.master)
	if !ms.checkIfMaster() {
		ms.electNewMaster()
	}
	// if !strings.HasPrefix(ms.ListSelf(), ms.master) {
	// 	ms.becomeMaster()
	// }
}

func (ms *MembershipService) handleOK() {
	ms.receivedOK = true
}
