package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Action struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	Command string `json:"name"`
	Payload string `json:"payload"`
}

func NewAction(id, kind, command, payload string) *Action {
	action := Action{
		Kind:    kind,
		Command: command,
		Payload: payload,
	}

	return &action
}

var f mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	// fmt.Printf("MSG: %s\n", msg.Payload())
}

func main() {
	reader := bufio.NewReader(os.Stdin)

	opts := mqtt.NewClientOptions().AddBroker("tcp://localhost:1883")
	opts.SetClientID("actions")
	opts.SetDefaultPublishHandler(f)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	for {
		text, err := reader.ReadString('\n')
		if err != nil {
			panic(err)
		}

		payload := strings.TrimSpace(text)
		action := createAction(payload)
		if action != nil {
			actionMsg, err := json.Marshal(action)
			if err != nil {
				panic(err)
			}
			token := client.Publish("/devices/test/actions", 1, false, string(actionMsg))
			token.Wait()
		}

		time.Sleep(1 * time.Second)
	}
}

func createAction(name string) *Action {
	id := generateID(10)
	fmt.Println("action =", name, "id =", id)
	switch name {
	case "ota":
		kind := "process"
		command := "tools/ota"
		payload := `{"hello": "world"}`
		action := NewAction(id, kind, command, payload)
		return action
	case "stop_collector":
		kind := "control"
		command := name
		payload := `{"hello": "world"}`
		action := NewAction(id, kind, command, payload)
		return action
	case "start_collector":
		kind := "control"
		command := name
		payload := `{"args": ["simulator"]}`
		action := NewAction(id, kind, command, payload)
		return action
	case "stop_collector_stream":
		kind := "control"
		command := name
		payload := `{"args": ["simulator", "gps"]}`
		action := NewAction(id, kind, command, payload)
		return action
	case "start_collector_strea":
		kind := "control"
		command := name
		payload := `{"args": ["simulator", "gps"]}`
		action := NewAction(id, kind, command, payload)
		return action
	default:
		fmt.Println("Invalid action")
		return nil
	}
}

func generateID(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
