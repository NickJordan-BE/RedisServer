package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// Constants for RESP types
const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
	NULL    = '_'
	BOOLEAN = '#'
	DOUBLES = ','
	MAP     = '%'
	ATTR    = '`'
	SETS    = '~'
)

// Value structure for RESP formatting and storage
type Value struct {
	typ    string
	str    string
	num    int
	double float64
	bulk   string
	array  []Value
	err    error
}

// need to add timestamping for TTL

// Reader for reading RESP
type Resp struct {
	reader *bufio.Reader
}

// Writer for writing RESP
type Writer struct {
	writer io.Writer
}

// Creates new reader
func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

// Created new writer
func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

// Decision structure for writing RESP protocol based on value type
// Returns a byte array representing the Value in RESP format
func (v Value) Marshal() []byte {
	switch v.typ {
	case "array":
		return v.marshalArray()
	case "string":
		return v.marshalString()
	case "error":
		return v.marshalError()
	case "bulk":
		return v.marshalBulk()
	case "null":
		return v.marshalNull()
	case "boolean":
		return v.marshalBoolean()
	case "double":
		return v.marshalDouble()
	case "map":
		return v.marshalMap()
	case "set":
		return v.marshalSet()
	case "integer":
		return v.marshalInteger()
	default:
		return []byte{}
	}
}

// RESP representation of a boolean return for writing
func (v Value) marshalBoolean() []byte {
	var bytes []byte

	bytes = append(bytes, BOOLEAN)
	bytes = append(bytes, strconv.Itoa(v.num)...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// RESP representation of a integer return for writing
func (v Value) marshalInteger() []byte {
	var bytes []byte

	bytes = append(bytes, INTEGER)
	bytes = append(bytes, strconv.Itoa(v.num)...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// RESP representation of a double return for writing
func (v Value) marshalDouble() []byte {
	var bytes []byte

	bytes = append(bytes, DOUBLES)
	bytes = append(bytes, strconv.FormatFloat(v.double, 'f', -1, 64)...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// RESP representation of a map return for writing
func (v Value) marshalMap() []byte {
	var bytes []byte

	bytes = append(bytes, MAP)
	bytes = append(bytes, strconv.Itoa(len(v.array))...)
	bytes = append(bytes, '\r', '\n')

	for _, element := range v.array {
		bytes = append(bytes, element.Marshal()...)
	}

	return bytes
}

// RESP representation of a set return for writing
func (v Value) marshalSet() []byte {
	var bytes []byte

	bytes = append(bytes, SETS)
	bytes = append(bytes, strconv.Itoa(len(v.array))...)
	bytes = append(bytes, '\r', '\n')

	for _, element := range v.array {
		bytes = append(bytes, element.Marshal()...)
	}

	return bytes
}

// Stores the values string for writing back to client
// Returns a byte representation of the string
func (v Value) marshalString() []byte {
	var bytes []byte

	bytes = append(bytes, STRING)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// Prepares the values bulk string for writing back to the client in RESP format
// Returns a byte representation in RESP format
func (v Value) marshalBulk() []byte {
	var bytes []byte

	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// Prepares the values array for writing back to the client in RESP format
// Returns a byte representation in RESP format
func (v Value) marshalArray() []byte {
	len := len(v.array)
	var bytes []byte

	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len; i++ {
		bytes = append(bytes, v.array[i].Marshal()...)
	}

	return bytes
}

// Handles simple errors in RESP format
// Returns byte representation in RESP format
func (v Value) marshalError() []byte {
	var bytes []byte

	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// Returns NULL value in RESP format for missing commands or returns
func (v Value) marshalNull() []byte {
	return []byte("$-1\r\n")
}

// Reads full RESP line
func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		byt, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, byt)

		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}

	return line[:len(line)-2], n, nil
}

// Parses and returns integer in RESP protocol
func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()

	if err != nil {
		return 0, 0, err
	}

	i64, err := strconv.ParseInt(string(line), 10, 64)

	if err != nil {
		return 0, n, err
	}

	return int(i64), n, nil
}

// Reads integer in RESP format and returns it
func (r *Resp) readIntegerRESP() (Value, error) {
	line, _, err := r.readLine()

	if err != nil {
		return Value{}, err
	}

	i64, err := strconv.ParseInt(string(line), 10, 64)

	if err != nil {
		return Value{}, err
	}

	return Value{typ: "integer", num: int(i64)}, nil
}

// Reads boolean in RESP format and returns it
func (r *Resp) readBoolean() (Value, error) {
	v := Value{}
	v.typ = "boolean"

	num, _, err := r.readInteger()

	if err != nil {
		return v, err
	}

	v.num = num

	return v, nil
}

// Reads double in RESP format and returns it
func (r *Resp) readDouble() (Value, error) {
	v := Value{}
	v.typ = "double"

	line, _, err := r.readLine()

	if err != nil {
		return Value{}, err
	}

	val, err := strconv.ParseFloat(string(line), 64)

	if err != nil {
		return Value{}, err
	}

	v.double = val

	return v, nil
}

// Reads map in RESP format and returns it
func (r *Resp) readMap() (Value, error) {
	v := Value{}
	v.typ = "map"

	length, _, err := r.readInteger()

	if err != nil {
		return Value{}, err
	}

	v.array = make([]Value, length*2)

	for i := 0; i < length; i++ {
		key, err := r.readBulk()

		if err != nil {
			return v, err
		}

		value, err := r.readBulk()

		if err != nil {
			return v, err
		}

		v.array[i*2] = key
		v.array[i*2+1] = value
	}

	return v, nil
}

// Reads set in RESP format and returns it
func (r *Resp) readSet() (Value, error) {
	v := Value{}
	v.typ = "set"

	length, _, err := r.readInteger()

	if err != nil {
		return v, err
	}

	v.array = make([]Value, 0)

	for i := 0; i < length; i++ {
		cur, err := r.Read()

		if err != nil {
			return v, nil
		}

		v.array[i] = cur
	}

	return v, nil
}

// Reads bulk string in RESP format and returns it
func (r *Resp) readBulk() (Value, error) {
	v := Value{}
	v.typ = "bulk"

	// Length of bulk string
	bytes, _, err := r.readInteger()

	if err != nil {
		return v, err
	}

	bul := make([]byte, bytes)

	// Read string
	r.reader.Read(bul)

	// Store string
	v.bulk = string(bul)

	// Read the trailing CRLF
	r.readLine()

	return v, nil
}

// Parsing RESP protocol for array and returns it
func (r *Resp) readArray() (Value, error) {
	v := Value{}
	v.typ = "array"

	length, _, err := r.readInteger()

	if err != nil {
		return v, err
	}

	v.array = make([]Value, length)

	// Reads values and stores into array
	for i := 0; i < length; i++ {
		cur, err := r.Read()

		if err != nil {
			return v, err
		}

		v.array[i] = cur
	}

	return v, nil
}

// Reads first byte in RESP protocol and executes proper
// reading of the data type
func (r *Resp) Read() (Value, error) {
	dataType, err := r.reader.ReadByte()

	if err != nil {
		return Value{}, err
	}

	switch dataType {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	case INTEGER:
		return r.readIntegerRESP()
	case BOOLEAN:
		return r.readBoolean()
	case DOUBLES:
		return r.readDouble()
	case MAP:
		return r.readMap()
	case SETS:
		return r.readSet()
	default:
		fmt.Println("Unknown")
		return Value{}, nil
	}
}

// Writes to console RESP format
func (w *Writer) Write(v Value) error {
	var bytes = v.Marshal()

	_, err := w.writer.Write(bytes)

	if err != nil {
		return err
	}

	return nil
}
