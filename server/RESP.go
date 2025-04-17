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
	typ   string
	str   string
	num   int
	bulk  string
	array []Value
}

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
	default:
		return []byte{}
	}
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
