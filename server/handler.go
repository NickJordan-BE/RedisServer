package main

import (
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// In memory Database DS
var SETs = map[string][2]string{}
var SETsMu = sync.RWMutex{}

// In memory hashmap for the DB
var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

// Stores streams
var XSETs = map[string]map[string]map[string]string{}
var XPREV = "0-0"
var XSETsMu = sync.RWMutex{}

// Stores for transactions
var QUEUE = make([][]Value, 0)
var queuing = false

// Ping Command
func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}

// Echo Command
func echo(args []Value) Value {
	v := Value{}
	v.typ = "string"
	var str string = ""

	for i := range args {
		str += args[i].bulk + " "
	}

	v.str = str

	return v
}

// Multiple sets at once for batch writes
func mSet(args []Value) Value {
	if len(args)%2 != 0 {
		return Value{typ: "string", str: "ERR: Mset requires an even nummber of arguments."}
	}
	count := 0

	SETsMu.Lock()
	for i := range args {
		list := make([]Value, 0)
		list = append(list, args[i])
		if _, ok := SETs[args[i].bulk]; !ok {
			count += 1
		}
		set(list)
	}

	return Value{typ: "integer", num: count}
}

// Multiple gets at once for batch reading
func mGet(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "ERR: Mget command needs at least one key"}
	}
	v := Value{}
	v.typ = "array"
	v.array = make([]Value, 0)

	for i := range args {
		list := make([]Value, 0)
		list = append(list, args[i])

		v.array = append(v.array, get(list))
	}

	return v
}

// Incr command
func incr(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "string", str: "ERR: Incorrect number of arguments for incr command"}
	}

	key := args[0].bulk

	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return Value{typ: "string", str: "Key Does not exist"}
	}

	num, err := strconv.Atoi(value[0])

	if err != nil {
		return Value{typ: "error", err: err}
	}

	num += 1

	SETsMu.Lock()
	SETs[key] = [2]string{strconv.Itoa(num), value[1]}
	SETsMu.Unlock()

	return Value{typ: "integer", num: num}
}

// Decr command
func decr(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "string", str: "ERR: Incorrect number of arguments for decr command"}
	}

	key := args[0].bulk

	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return Value{typ: "string", str: "Key Does not exist"}
	}

	num, err := strconv.Atoi(value[0])

	if err != nil {
		return Value{typ: "error", err: err}
	}

	num -= 1

	SETsMu.Lock()
	SETs[key] = [2]string{strconv.Itoa(num), value[1]}
	SETsMu.Unlock()

	return Value{typ: "integer", num: num}
}

// Time to live command
func TTL(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "string", str: "ERR: Wrong number of arguments for TTL command"}
	}

	v := Value{typ: "integer"}
	now := time.Now().Unix()
	value, ok := SETs[args[0].bulk]

	SETsMu.RLock()
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
	SETsMu.RUnlock()

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
	SETsMu.RLock()
	if _, ok := SETs[args[0].bulk]; ok {
		v.num = 1
	}
	SETsMu.RUnlock()

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

	// Creates pattern checker
	pattern := args[0].bulk
	regexPattern := globToRegex(pattern)
	re, err := regexp.Compile(regexPattern)

	if err != nil {
		return Value{typ: "error", err: err}
	}

	valueList := make([]Value, 0)

	// Checks patterns
	for key, _ := range SETs {
		if re.MatchString(key) {
			valueList = append(valueList, Value{typ: "bulk", bulk: key})
		}
	}

	return Value{typ: "array", array: valueList}
}

// Returns info on redis instance
func info(args []Value) Value {
	// Return replication info only
	v := Value{}

	if len(args) == 1 && args[0].bulk == "replication" {
		v.typ = "array"
		v.array = make([]Value, 0)
		v.array = append(v.array, Value{typ: "bulk",
			bulk: "master_replid:" +
				RedisInstance.master_replid})
		v.array = append(v.array, Value{typ: "bulk",
			bulk: "master_repl_offset:" +
				string(RedisInstance.master_repl_offset)})
	} else {
		v.typ = "bulk"
		v.bulk = "role:" + RedisInstance.role
	}

	return v
}

// Used for initiating handshake between replica and master
func replconf(args []Value) Value {
	return Value{typ: "string", str: "OK"}
}

// Used for initiating handshake between replica and master
func psync(args []Value) Value {
	return Value{typ: "string", str: "FULLRESYNC " + RedisInstance.master_replid + " " + string(RedisInstance.master_repl_offset)}
}

// Returns the type stored
func typeC(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "string", str: "Err not the correct number of args for type command"}
	}
	val := get(args)
	v := Value{typ: "string"}

	switch val.typ {
	case "bulk":
		v.str = "string"
	case "null":
		v.str = "none"
	case "array":
		v.str = "hash"
	}

	return v
}

// Adds stream entry into any identified stream
func xadd(args []Value) Value {
	hashMap := args[0].bulk
	id := args[1].bulk
	index := strings.Index(XPREV, "-")

	if XPREV[:index] > id[:index] {
		return Value{typ: "string", str: "Err"}
	} else if XPREV[index+1:] > id[index+1:] {
		return Value{typ: "string", str: "err"}
	}
	XPREV = id

	// Lock due to multiple connections
	XSETsMu.Lock()
	if _, ok := HSETs[hashMap]; !ok {
		XSETs[hashMap] = map[string]map[string]string{}
	}

	if _, ok := HSETs[hashMap][id]; !ok {
		XSETs[hashMap][id] = map[string]string{}
	}

	for i := 2; i < len(args)-2; i++ {
		key := args[i].bulk
		val := args[i+1].bulk

		XSETs[hashMap][id][key] = val
	}

	XSETsMu.Unlock()

	return Value{typ: "bulk", bulk: id}
}

// Starts transaction
func multi(args []Value) Value {
	if len(args) != 0 {
		return Value{typ: "string", str: "Err"}
	}
	queuing = true
	return Value{typ: "string", str: "OK"}
}

// Executes stored transactions
func exec(args []Value) Value {
	if len(args) != 0 {
		return Value{typ: "string", str: "Err"}
	}
	if len(QUEUE) == 0 {
		return Value{typ: "string", str: "EXEC without Multi"}
	}

	results := make([]Value, 0)
	queuing = false

	for i := range QUEUE {
		command := strings.ToUpper(QUEUE[i][0].bulk)
		args := QUEUE[i][1:]
		handler, ok := Handlers[command]

		if !ok {
			results = append(results, Value{typ: "string", str: "ERR Unknown Command"})
			continue
		}

		results = append(results, handler(args))
	}

	QUEUE = make([][]Value, 0)

	return Value{typ: "array", array: results}
}

// Commmand handler variable initialized after all functions are defined
var Handlers map[string]func([]Value) Value

// handles function calls for commands
func init() {
	Handlers = map[string]func([]Value) Value{
		"PING":     ping,
		"ECHO":     echo,
		"SET":      set,
		"GET":      get,
		"HSET":     hSet,
		"HGET":     hGet,
		"HGETALL":  hGetAll,
		"CONFIG":   config,
		"KEYS":     keys,
		"INFO":     info,
		"DEL":      del,
		"EXISTS":   exists,
		"TTL":      TTL,
		"INCR":     incr,
		"DECR":     decr,
		"MGET":     mGet,
		"MSET":     mSet,
		"REPLCONF": replconf,
		"PSYNC":    psync,
		"TYPE":     typeC,
		"XADD":     xadd,
		"MULTI":    multi,
		"EXEC":     exec,
	}
}
