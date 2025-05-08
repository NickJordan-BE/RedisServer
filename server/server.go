package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

type Redis struct {
	role               string
	master_replid      string
	master_repl_offset byte
	masterIP           string
	is_rep             int
	master_host        string
	master_port        string
}

var RedisInstance = &Redis{
	role:               "master",
	master_replid:      "",
	master_repl_offset: 0,
	masterIP:           "",
	is_rep:             0,
	master_host:        "",
	master_port:        "",
}

func main() {
	defaultPort := ":6379"
	var port string

	if len(os.Args) == 1 {
		port = defaultPort
	} else {
		port = ":" + os.Args[2]
	}

	if len(os.Args) > 3 && os.Args[3] == "--replicaof" {
		masterInfo := os.Args[4]

		RedisInstance.role = "slave"
		RedisInstance.is_rep = 1
		spaceIndex := strings.Index(masterInfo, " ")

		RedisInstance.master_host = masterInfo[:spaceIndex]
		RedisInstance.master_port = ":" + masterInfo[spaceIndex:]

		// GoRoutine to initiate replication handshake
		initiateHandshake(RedisInstance)
	} else {
		RedisInstance.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	}

	// Establish port connection
	server, err := net.Listen("tcp", port)
	fmt.Println("Listening on port ", port)

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

	for {
		// Listen for connections
		conn, err := server.Accept()

		if err != nil {
			fmt.Println(err)
			return
		}

		go handleClient(conn, aof)
	}

}

// Go routine that handles multiple client connections to the server
func handleClient(conn net.Conn, aof *Aof) {
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
			fmt.Printf("Error reading from client %s: %s\n", conn.RemoteAddr(), err.Error())
			return
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
			writer.Write(Value{typ: "string", str: "ERR Unknown Command"})
			continue
		}

		// Write to aof file if command is a write
		if command == "HSET" || command == "SET" || command == "DEL" || command == "MSET" || command == "INCR" || command == "DECR" || command == "XADD" {
			aof.Write(value)
		}

		// Responding to client
		res := handler(args)
		writer.Write(res)
	}
}

// Initiates handshake with master client for replication
func initiateHandshake(RedisInstance *Redis) {
	master, err := net.Dial("tcp", RedisInstance.master_host+RedisInstance.master_port)

	if err != nil {
		fmt.Print("Dial Error", err)
		return
	}
	defer master.Close()

	commands := make([][]byte, 0)
	commands = append(commands, []byte("*1\r\n$4\r\nping\r\n"))
	commands = append(commands, []byte(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n%s\r\n", RedisInstance.master_port)))
	commands = append(commands, []byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	commands = append(commands, []byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))

	resp := NewResp(master)

	for i := range commands {
		time.Sleep(time.Second * 1)
		master.Write(commands[i])
		_, err := resp.Read()

		if err != nil {
			fmt.Print("Error", err)
			return
		}
	}
}
