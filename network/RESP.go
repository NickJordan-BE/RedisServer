package main

import (
	"bufio"
	"strings"
	"strconv"
	"fmt"
	"os"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK	= '$'
	ARRAY 	= '*'
	NULL	= '_'
	BOOLEAN = '#'
	DOUBLES = ','
	MAP		= '%'
	ATTR	= '`'
	SETS	= '~'
)

type Value struct {
	typ string
	str string
	num int
	bulk string
	array []Value
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		byt, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, byt)

		if len(line) >= 2 && line[len(line) - 2] == '\r' {
			break
		}
	}

	return line[:len(line)-2], n, nil
}

func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err = r.readLine()

	if err != nil {
		return 0, 0, err
	}

	i64, err := strings.ParseInt(string(line), 10, 64)

	if err != nil {
		return 0, n, err
	}

	return int(i64), n, nil
}

func (r *Resp) readArray() (Value, error) {
	v := Value{}
	v.type = "array"

	length, _, err := r.readInteger()

	if err != nil {
		return v, err
	}

	v.array := make([]Value, length)

	for i := 0; i < length; i++ {
		cur, err := r.Read()

		if err != nil {
			return v, err
		}

		v.array[i] = cur
	}

	return v, nil
}

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
		return r.readInteger()
	}
}
input := "$5\r\nAhmed\r\n"

reader := bufio.NewReader(strings.NewReader(input))

dataType, _ := reader.ReadByte()

if dataType != '$' {
	fmt.Println("Invalid type, expecting bulk strings only")
	os.Exit(1)
}

size, _ := reader.ReadByte()

strSize, _ := strconv.ParseInt(string(size), 10, 64)

reader.ReadByte()
reader.ReadByte()

name := make([]byte, strSize)
reader.Read(name)

fmt.Println(string(name))