package main

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
)

type ActionStatus struct {
	Stream    string   `json:"stream"`
	Sequence  int32    `json:"sequence"`
	Timestamp int64    `json:"timestamp"`
	Id        string   `json:"action_id"`
	State     string   `json:"state"`
	Progress  int8     `json:"progress"`
	Errors    []string `json:"errors"`
}

type Action struct {
	Id      string	`json:"action_id"`
	Kind    string  `json:"timestamp"`
	Name    string  `json:"name"`
	Payload string  `json:"payload"`
}

func main() {
	// Connect to uplink via bridge port
	c, err := net.Dial("tcp", "localhost:5050")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c.Close()

	fmt.Printf("Connected to %s\n", c.RemoteAddr().String())
	reader := json.NewDecoder(c)
	writer := json.NewEncoder(c)
	for {
		// Read Action from uplink
		var action Action
		if err := reader.Decode(&action); err != nil {
			fmt.Println("failed to unmarshal:", err)
			continue
		} else {
			fmt.Println(action)
		}

		// Respond as Completed
		reply := ActionStatus {
			Stream: "action_status",
			Sequence: 1,
			Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
			Id: action.Id,
			State: "Completed",
			Progress: 100,
			Errors: []string{},
		}

		fmt.Println(reply)

		err := writer.Encode(reply)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}
