package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"time"
)

type ActionStatus struct {
	Stream    string `json:"stream"`
	Sequence  int32  `json:"sequence"`
	Timestamp int64  `json:"timestamp"`
	Payload   string `json:"payload"`
}

func main() {
	// Connect to uplink via bridge port
	c, err := net.Dial("tcp", "localhost:5555")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c.Close()

	fmt.Printf("Connected to %s\n", c.RemoteAddr().String())
	for {
		// Read data from uplink
		netData, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println(netData)

		// Reply that serves no other purpose but as a ping before timeout
		reply, err := json.Marshal(ActionStatus{
			Stream:    "action_status",
			Sequence:  1,
			Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
			Payload:   "",
		})
		if err != nil {
			fmt.Println(err)
			return
		}
		status := string(reply)

		fmt.Println(status)

		n, err := bufio.NewWriter(c).WriteString(status)
		_ = n
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}
