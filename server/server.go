package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

func main() {
	fmt.Println("Listening on port :6379")

	// Establish port connection
	server, err := net.Listen("tcp", ":6379")

	if err != nil {
		fmt.Println(err)
		return
	}

	// Create or open aof file for AOS protocol
	aof, err := NewAof("database.aof")

	if err != nil {
		fmt.Println(err)
		return
	}

	defer aof.Close()

	// Read all write commands from file
	aof.Read(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]

		if !ok {
			fmt.Println("Invalid Command: ", command)
			return
		}

		handler(args)
	})

	// Listen for connections
	conn, err := server.Accept()

	if err != nil {
		fmt.Println(err)
		return
	}

	// Close on end of connection
	defer conn.Close()

	for {
		// Allow RESP to recieve requests
		resp := NewResp(conn)

		// RESP serialize request into RESP array
		value, err := resp.Read()

		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading from client: ", err.Error())
			os.Exit(1)
		}

		if value.typ != "array" {
			fmt.Println("Invalid Request")
			continue
		}

		if len(value.array) == 0 {
			fmt.Println("Array cannot be empty.")
			continue
		}

		// Connecting writer to port
		writer := NewWriter(conn)

		// Decoding request from RESP array
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		// Checking command is valid and grabbing function
		handler, ok := Handlers[command]

		if !ok {
			fmt.Println("Invalid Command: ", command)
			writer.Write(Value{typ: "string", str: ""})
			continue
		}

		// Write to aof file if command is a write
		if command == "HSET" || command == "SET" {
			aof.Write(value)
		}

		// Responding to client
		res := handler(args)
		writer.Write(res)
	}
}
