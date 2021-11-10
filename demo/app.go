package main

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
)

// Struct used to transport ActionResonses/data to bridge
type Payload struct {
	Stream    string                 `json:"stream"`
	Sequence  int32                  `json:"sequence"`
	Timestamp int64                  `json:"timestamp"`
	Payload   map[string]interface{} `json:"payload"`
}

// Struct to notify status of action in execution
type ActionResponse struct {
	Id        string   `json:"action_id"`
	Timestamp int64    `json:"timestamp"`
	State     string   `json:"state"`
	Progress  int8     `json:"progress"`
	Errors    []string `json:"errors"`
}

// Struct received from uplink
type Action struct {
	Id      string `json:"action_id"`
	Kind    string `json:"timestamp"`
	Name    string `json:"name"`
	Payload string `json:"payload"`
}

// Generate ActionResponse payload provided executing action information
func actionResponse(action_id string, state string, progresss int8) Payload {
	t := time.Now().UnixNano() / int64(time.Millisecond)
	response := ActionResponse{
		Id:        action_id,
		Timestamp: t,
		State:     state,
		Progress:  progresss,
		Errors:    []string{},
	}
	var resp map[string]interface{}
	r, err := json.Marshal(response)
	json.Unmarshal(r, &resp)
	if err != nil {
		fmt.Println(err)
	}
	return payload("action_status", resp)
}

// Generate payload provided stream and data values
func payload(stream string, data map[string]interface{}) Payload {
	return Payload{
		Stream:    stream,
		Sequence:  0,
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
		Payload:   data,
	}
}

// Creates and sends template status, with provided state and progress
func reply(writer *json.Encoder, reply Payload) {
	// Sleep for 5s
	time.Sleep(5)

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
		status := actionResponse(action.Id, "Running", 0)
		reply(writer, status)
		// Status: completed execution
		status = actionResponse(action.Id, "Completed", 100)
		reply(writer, status)
	}
}
