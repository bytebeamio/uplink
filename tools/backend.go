package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/api/v1/devices/action/status", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println(r)
		fmt.Fprintf(w, "OK")
	})

	log.Fatal(http.ListenAndServe(":3333", nil))
}
