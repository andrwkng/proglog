package main

import (
	"log"

	"github.com/andrwkng/proglog/internal/server"
)

func main() {
	s := server.NewHTTPServer(":8080")
	log.Fatal(s.ListenAndServe())
}
