package main

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
)

// Struct used to transport ActionResonses/data to bridge
type Payload struct {
	Stream    string       `json:"stream"`
	Sequence  int32        `json:"sequence"`
	Timestamp int64        `json:"timestamp"`
	Payload   ActionStatus `json:"payload"`
}

// Struct to notify status of action in execution
type ActionStatus struct {
	Id        string   `json:"action_id"`
	Timestamp int64    `json:"timestamp"`
	State     string   `json:"state"`
	Progress  int8     `json:"progress"`
	Errors    []string `json:"errors"`
}

// Struct received from uplink
type Action struct {
	Id      string `json:"id"`
	Kind    string `json:"timestamp"`
	Name    string `json:"name"`
	Payload string `json:"payload"`
}

// Creates and sends template status, with provided state and progress
func reply(writer *json.Encoder, action_id string, state string, progresss int8) {
	// Sleep for 5s
	time.Sleep(5)

	// Reply that serves no other purpose but as a ping before timeout
	reply := Payload{
		Stream:    "action_status",
		Sequence:  0,
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
		Payload: ActionStatus{
			Id:        action_id,
			Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
			State:     state,
			Progress:  progresss,
		},
	}

	fmt.Println(reply)

	// Send data to uplink
	err := writer.Encode(reply)
	if err != nil {
		fmt.Println(err)
		return
	}
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
	// Create new handlers for encoding and decoding JSON
	reader := json.NewDecoder(c)
	writer := json.NewEncoder(c)
	for {
		// Read data from uplink
		var action Action
		if err := reader.Decode(&action); err != nil {
			fmt.Println("failed to unmarshal:", err)
		} else {
			fmt.Println(action)
		}

		// Status: started execution
		reply(writer, action.Id, "Running", 0)
		// Status: completed execution
		reply(writer, action.Id, "Completed", 100)
	}
}
