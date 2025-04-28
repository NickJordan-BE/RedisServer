package main

import (
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Redis struct {
	role     string
	masterIP string
}

var SETs = map[string][2]string{}
var SETsMu = sync.RWMutex{}

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

var redis = &Redis{
	role:     "master",
	masterIP: "",
}

// Ping Command
func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}

// Echo Command
func echo(args []Value) Value {
	return Value{typ: "string", str: args[0].bulk}
}

// Time to live command
func TTL(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "string", str: "ERR: Wrong number of arguments for TTL command"}
	}

	v := Value{typ: "integer"}
	now := time.Now().Unix()
	value, ok := SETs[args[0].bulk]

	SETsMu.Lock()
	if ok {
		timestamp, err := strconv.Atoi(value[1])
		num := int64(timestamp) - now
		if err != nil {
			return Value{typ: "error", err: err}
		}

		v.num = int(num)
	} else {
		return Value{typ: "null"}
	}
	SETsMu.Unlock()

	return v
}

// Checks if key exists returning boolean
func exists(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR: Wrong number of arguments for exists command"}
	}

	v := Value{}
	v.typ = "integer"
	v.num = 0
	SETsMu.Lock()
	if _, ok := SETs[args[0].bulk]; ok {
		v.num = 1
	}
	SETsMu.Unlock()

	return v
}

// Safe deletes from map and returns number deleted
func del(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "error", str: "ERR: Wrong number of arguments for del command"}
	}

	numDel := 0

	SETsMu.Lock()
	for i := 0; i < len(args); i++ {
		if _, ok := SETs[args[i].bulk]; ok {
			delete(SETs, args[i].bulk)
			numDel += 1
		}
	}
	SETsMu.Unlock()

	return Value{typ: "integer", num: numDel}
}

// Set command
func set(args []Value) Value {
	if len(args) != 2 && len(args) != 4 {
		return Value{typ: "error", str: "ERR: Wrong number of arguments for set command"}
	}

	key := args[0].bulk
	value := args[1].bulk
	var expireTime int64

	// Checks for expiration argmuments
	if len(args) == 4 && strings.ToUpper(args[2].bulk) == "PX" {
		if exp, err := strconv.Atoi(args[3].bulk); err == nil {
			// Concurrent function sleeps for given milliseconds
			// Then removes key from cache
			expireTime = time.Now().Add(time.Duration(exp) * time.Second).Unix()
			go func(key string, duration int) {
				time.Sleep(time.Duration(duration) * time.Second)
				SETsMu.Lock()
				delete(SETs, key)
				SETsMu.Unlock()
			}(key, exp)
		}

	}
	// Locking because of concurrent connections
	SETsMu.Lock()
	SETs[key] = [2]string{value, strconv.Itoa(int(expireTime))}
	SETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

// GET Command
func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "string", str: "ERR: Wrong number of arguments for get command"}
	}

	key := args[0].bulk

	// Lock due to multiple concurrent connections
	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value[0]}
}

// HSET command
func hSet(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "string", str: "ERR: Wrong number of arguments for hset command"}
	}

	hashMap := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	// Lock due to multiple connections
	HSETsMu.Lock()
	if _, ok := HSETs[hashMap]; !ok {
		HSETs[hashMap] = map[string]string{}
	}
	HSETs[hashMap][key] = value
	HSETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

// HGET command
func hGet(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "string", str: "ERR: Wrong number of arguments for hget command"}
	}

	hashMap := args[0].bulk
	key := args[1].bulk

	// Lock due to multiple connections
	HSETsMu.RLock()
	value, ok := HSETs[hashMap][key]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

// HGETALL Command
func hGetAll(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "string", str: "ERR: Wrong number of arguments for hgetall command"}
	}

	hashMap := args[0].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hashMap]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	var values = make([]Value, len(value)*2)
	j := 0
	for key, v := range value {
		values[j] = Value{typ: "bulk", bulk: key}
		values[j+1] = Value{typ: "bulk", bulk: v}
		j += 2
	}

	return Value{typ: "array", array: values}
}

// config command
func config(args []Value) Value {
	if len(args) != 2 && len(args) != 3 {
		return Value{typ: "string", str: "ERR: Wrong number of arguments for the config command"}
	}

	dir := "dir"
	dirR := "/tmp/redis-data"
	dbFileName := "dbfilename"
	dbFileNameR := "dump.rdb"

	list := make([]Value, 0)

	switch strings.ToUpper(args[0].bulk) {
	case "GET":
		if args[1].bulk == dir || len(args) == 3 && args[2].bulk == dir {
			list = append(list, Value{typ: "bulk", bulk: dir})
			list = append(list, Value{typ: "bulk", bulk: dirR})
		}
		if args[1].bulk == dbFileName || len(args) == 3 && args[2].bulk == dbFileName {
			list = append(list, Value{typ: "bulk", bulk: dbFileName})
			list = append(list, Value{typ: "bulk", bulk: dbFileNameR})
		}

		return Value{typ: "array", array: list}
	case "SET":
		return Value{}
	default:
		return Value{typ: "error", str: "ERR: unsupported CONFIG Parameter"}
	}
}

// Keys command supports glob style
func keys(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "string", str: "ERR: Wrong number of arguments for the keys command"}
	}

	pattern := args[0].bulk
	regexPattern := globToRegex(pattern)
	re, err := regexp.Compile(regexPattern)

	if err != nil {
		return Value{typ: "error", err: err}
	}

	valueList := make([]Value, 0)

	for key, _ := range SETs {
		if re.MatchString(key) {
			valueList = append(valueList, Value{typ: "bulk", bulk: key})
		}
	}

	return Value{typ: "array", array: valueList}
}

func info(args []Value) Value {
	return Value{typ: "bulk", bulk: "role:" + redis.role}
}

// handles function calls for commands
var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"ECHO":    echo,
	"SET":     set,
	"GET":     get,
	"HSET":    hSet,
	"HGET":    hGet,
	"HGETALL": hGetAll,
	"CONFIG":  config,
	"KEYS":    keys,
	"INFO":    info,
	"DEL":     del,
	"EXISTS":  exists,
	"TTL":     TTL,
}
