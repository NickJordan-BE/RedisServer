package main

import (
	"fmt"
	"net"
	"io"
	"os"
	"bufio"
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
		message, err := bufio.NewReader(conn).ReadString('\n')

		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading from client: ", err.Error())
			os.Exit(1)
		}

		conn.Write([]byte("+PONG\r\n"))
	}
}