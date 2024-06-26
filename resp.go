/*
This is a partial implementation of the Redis Serialization Protocol (RESP) for
educational purposes. RESP is used by Redis and supports different data types 
including Simple Strings, Errors, Integers, Bulk Strings, and Arrays. For a 
detailed description of the protocol and its data types, refer to the following 
documentation:

https://redis.io/docs/latest/develop/reference/protocol-spec/#resp-protocol-description
*/

package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// First byte of each RESP data type
const (
	FB_SIMPLE_STRING = '+'
	FB_SIMPLE_ERROR  = '-'
	FB_INTEGER       = ':'
	FB_BULK_STRING   = '$'
	FB_ARRAY         = '*'
)

// ValueTyp represents the type of RESP value
type ValueTyp string

const (
	ValueTypSimpleString ValueTyp = "SIMPLE_STRING"
	ValueTypSimpleError  ValueTyp = "SIMPLE_ERROR"
	ValueTypInteger      ValueTyp = "INTEGER"
	ValueTypBulkString   ValueTyp = "BULK_STRING"
	ValueTypArray        ValueTyp = "ARRAY"
	ValueTypNull         ValueTyp = "NULL"
)

// Value holds the parsed RESP data
type Value struct {
	typ   ValueTyp
	str   string
	num   int
	bulk  string
	array []Value
}

// Resp represents a RESP parser
type Resp struct {
	reader *bufio.Reader
}

// NewResp creates a new Resp parser
func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

// readLine reads a line ending with \r\n
func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n++
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}
	return line[:len(line)-2], n, nil
}

// readInteger reads an integer from the RESP data
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

// Read reads a RESP value
func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch _type {
	case FB_ARRAY:
		return r.readArray()
	case FB_BULK_STRING:
		return r.readBulkString()
	default:
		fmt.Printf("Unknown type: %v\n", string(_type))
		return Value{}, nil
	}
}

// readArray reads an array from the RESP data
func (r *Resp) readArray() (Value, error) {
	v := Value{typ: ValueTypArray}

	// read the length of the array
	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	// parse and read each value in the array
	v.array = make([]Value, 0, length)
	for i := 0; i < length; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}
		v.array = append(v.array, val)
	}

	return v, nil
}

// readBulkString reads a bulk string from the RESP data
func (r *Resp) readBulkString() (Value, error) {
	v := Value{typ: ValueTypBulkString}

	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, length)
	_, err = r.reader.Read(bulk)
	if err != nil {
		return v, err
	}
	v.bulk = string(bulk)

	// Read the trailing CRLF (\r\n)
	_, _, err = r.readLine()

	return v, err
}

// Marshal marshals the RESP value to bytes
func (v Value) Marshal() []byte {
	switch v.typ {
	case ValueTypArray:
		return v.marshalArray()
	case ValueTypBulkString:
		return v.marshalBulkString()
	case ValueTypSimpleString:
		return v.marshalSimpleString()
	case ValueTypNull:
		return v.marshalNull()
	case ValueTypSimpleError:
		return v.marshalError()
	default:
		return []byte{}
	}
}

// marshalSimpleString marshals a simple string value
func (v Value) marshalSimpleString() []byte {
	return append([]byte{FB_SIMPLE_STRING}, append([]byte(v.str), '\r', '\n')...)
}

// marshalBulkString marshals a bulk string value
func (v Value) marshalBulkString() []byte {
	return append(append(append([]byte{FB_BULK_STRING}, strconv.Itoa(len(v.bulk))...), '\r', '\n'), append([]byte(v.bulk), '\r', '\n')...)
}

// marshalArray marshals an array value
func (v Value) marshalArray() []byte {
	bytes := append([]byte{FB_ARRAY}, strconv.Itoa(len(v.array))...)
	bytes = append(bytes, '\r', '\n')
	for _, val := range v.array {
		bytes = append(bytes, val.Marshal()...)
	}
	return bytes
}

// marshalError marshals an error value
func (v Value) marshalError() []byte {
	return append([]byte{FB_SIMPLE_ERROR}, append([]byte(v.str), '\r', '\n')...)
}

// marshalNull marshals a null value
func (v Value) marshalNull() []byte {
	return []byte("$-1\r\n")
}

// Writer represents a RESP writer
type Writer struct {
	writer io.Writer
}

// NewWriter creates a new Writer
func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

// Write writes a RESP value to the writer
func (w *Writer) Write(v Value) error {
	_, err := w.writer.Write(v.Marshal())
	return err
}
