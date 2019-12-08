package membership

import (
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
)

type message struct {
	tag     int
	content string
}

func createMessage(tag int, content string) message {
	msg := message{
		tag:     tag,
		content: content,
	}
	return msg
}

func (ms *MembershipService) sendMessage(msg message, addr *net.UDPAddr) {
	randNum := rand.Intn(100)
	if randNum < ms.lossRate {
		// log.Printf("Packet lost")
	} else {
		_, err := ms.conn.WriteToUDP([]byte(encodeMessage(msg)), addr)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func decodeMessage(str string) message {
	strs := strings.Split(str, ";")

	tag, err := strconv.Atoi(strs[0])
	if err != nil {
		log.Fatal(err)
	}

	msg := createMessage(tag, strs[1])
	return msg
}

func encodeMessage(msg message) string {
	strs := []string{strconv.Itoa(msg.tag), msg.content}
	return strings.Join(strs, ";")
}
