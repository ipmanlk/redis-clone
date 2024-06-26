/*
This file contains the implementation of various command handlers for the RESP
protocol. These handlers process commands such as PING, SET, GET, HSET, HGET, 
and HGETALL, providing basic functionalities similar to those found in Redis. 
The handlers manage simple key-value pairs and hash maps using in-memory storage.
*/

package main

import "sync"

// Handlers maps command strings to their respective handler functions.
var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
}

// SETs stores key-value pairs for the SET command.
var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

// HSETs stores hash maps for the HSET command.
var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

// ping handles the PING command.
func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: ValueTypSimpleString, str: "PONG"}
	}
	return Value{typ: ValueTypSimpleString, str: args[0].bulk}
}

// set handles the SET command.
func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: ValueTypSimpleError, str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	value := args[1].bulk

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	return Value{typ: ValueTypSimpleString, str: "OK"}
}

// get handles the GET command.
func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: ValueTypSimpleError, str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk

	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return Value{typ: ValueTypNull}
	}

	return Value{typ: ValueTypBulkString, bulk: value}
}

// hset handles the HSET command.
func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: ValueTypSimpleError, str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMu.Unlock()

	return Value{typ: ValueTypSimpleString, str: "OK"}
}

// hget handles the HGET command.
func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: ValueTypSimpleError, str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: ValueTypNull}
	}

	return Value{typ: ValueTypBulkString, bulk: value}
}

// hgetall handles the HGETALL command.
func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: ValueTypSimpleError, str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := args[0].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: ValueTypNull}
	}

	values := make([]Value, 0, len(value)*2)
	for k, v := range value {
		values = append(values, Value{typ: ValueTypBulkString, bulk: k})
		values = append(values, Value{typ: ValueTypBulkString, bulk: v})
	}

	return Value{typ: ValueTypArray, array: values}
}
