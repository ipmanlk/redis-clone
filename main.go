package main

import (
	"fmt"
	"net"
	"strings"
)

func main() {
	fmt.Println("Listening on port :6379")

	// Create a TCP listener on port 6379
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Error starting TCP listener:", err)
		return
	}
	defer l.Close()

	// Initialize the AOF (Append Only File) for persistence
	aof, err := NewAof("database.aof")
	if err != nil {
		fmt.Println("Error initializing AOF:", err)
		return
	}
	defer aof.Close()

	aof.Read(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			return
		}

		handler(args)
	})

	// Accept connections in a loop
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		// Handle each connection in a separate goroutine
		go handleConnection(conn, aof)
	}
}

// handleConnection handles RESP commands from a single client connection.
func handleConnection(conn net.Conn, aof *Aof) {
	defer conn.Close() // Ensure the connection is closed when the function returns

	resp := NewResp(conn)
	writer := NewWriter(conn)

	for {
		// Read the next RESP value from the connection
		value, err := resp.Read()
		if err != nil {
			fmt.Println("Error reading from connection:", err)
			return
		}

		// Validate that the value is an array
		if value.typ != ValueTypArray {
			fmt.Println("Invalid request, expected array")
			writer.Write(Value{typ: ValueTypSimpleError, str: "ERR invalid request, expected array"})
			continue
		}

		// Ensure the array has at least one element (the command)
		if len(value.array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			writer.Write(Value{typ: ValueTypSimpleError, str: "ERR invalid request, expected array length > 0"})
			continue
		}

		// Extract the command and arguments from the array
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		// Find the handler for the command
		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command:", command)
			writer.Write(Value{typ: ValueTypSimpleError, str: "ERR unknown command"})
			continue
		}

		// Write the command to the AOF for persistence if it is a modifying command
		if command == "SET" || command == "HSET" {
			if err := aof.Write(value); err != nil {
				fmt.Println("Error writing to AOF:", err)
				writer.Write(Value{typ: ValueTypSimpleError, str: "ERR failed to persist data"})
				continue
			}
		}

		// Execute the command handler and write the result to the client
		result := handler(args)
		writer.Write(result)
	}
}
