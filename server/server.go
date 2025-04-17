package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	fmt.Println("Listening on port :6379")

	server, err := net.Listen("tcp", ":6379")

	if err != nil {
		fmt.Println(err)
		return
	}

	conn, err := server.Accept()

	if err != nil {
		fmt.Println(err)
		return
	}

	defer conn.Close()

	for {
		resp := NewResp(conn)

		message, err := resp.Read()

		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading from client: ", err.Error())
			os.Exit(1)
		}

		_ = message

		writer := NewWriter(conn)
		writer.Write(Value{typ: "string", str: "OK"})
	}
}
