package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"
)

type ActionStatus struct {
	Id       string   `json:"id"`
	Timestamp int64   `json:"timestamp"`
	State    string   `json:"state"`
	Progress string   `json:"progress"`
	Errors   []string `json:"errors"`
}

func main() {
	if len(os.Args) != 3 {
		status := ActionStatus{
			Id:       "None",
			Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
			State:    "Completed",
			Progress: "0",
			Errors:   []string{"Expected 2 arguments"},
		}

		o, err := json.Marshal(status)
		if err != nil {
			panic(err)
		}

		fmt.Println(string(o))
		return
	}

	id := os.Args[1]
	for i := 0; i < 100; i++ {
		status := ActionStatus{
			Id:       id,
			Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
			State:    "In Progress",
			Progress: strconv.Itoa(i),
			Errors:   []string{},
		}

		o, err := json.Marshal(status)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(o))
		time.Sleep(1 * time.Second)
	}

	status := ActionStatus{
		Id:       id,
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
		State:    "Completed",
		Progress: "100",
		Errors:   []string{},
	}

	o, err := json.Marshal(status)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(o))
}
