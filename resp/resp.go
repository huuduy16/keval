package resp

import (
	"bufio"
	"io"
	"log"
	"strconv"
)

const (
	Sign_STRING      byte = '+'
	Sign_ERROR       byte = '-'
	Sign_INTEGER     byte = ':'
	Sign_BULK_STRING byte = '$'
	Sign_ARRAY       byte = '*'
)

const (
	STRING      string = "string"
	ERROR       string = "error"
	INTEGER     string = "integer"
	BULK_STRING string = "bulk string"
	ARRAY       string = "array"
	NULL        string = "null"
)

type Value struct {
	Ki      string
	Str     string
	Num     int64
	BulkStr string
	Array   []Value
}

type Writer struct {
	writer io.Writer
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(ioRd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(ioRd)}
}

func (r *Resp) readLine() (line []byte, n int, err error) {
	//line, _, err = r.reader.ReadLine()
	//n = len(line)
	var b byte
	for {
		b, err = r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n++
		if b == '\n' && len(line) > 0 && line[len(line)-1] == '\r' {
			break
		}
		line = append(line, b)
	}

	line, err = line[:len(line)-1], nil
	return
}

func (r *Resp) readInt() (x int64, n int, err error) {
	var line []byte
	line, n, err = r.readLine()
	if err != nil {
		return 0, 0, err
	}
	x, err = strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	err = nil
	return
}

func (r *Resp) ReadArray() (rs Value, err error) {
	rs.Ki = ARRAY
	var size int64
	size, _, err = r.readInt()
	if err != nil {
		return
	}

	rs.Array = make([]Value, size)
	var tmp Value
	for i := range rs.Array {
		tmp, err = r.Read()
		if err != nil {
			return
		}
		rs.Array[i] = tmp
	}
	return
}

func (r *Resp) ReadBulkStr() (rs Value, err error) {
	rs.Ki = BULK_STRING
	var size int64
	size, _, err = r.readInt()
	if err != nil {
		return
	}

	buffBulk := make([]byte, size)
	_, err = r.reader.Read(buffBulk)
	if err != nil {
		return
	}
	rs.BulkStr = string(buffBulk)
	// to skip \r\n
	_, _, _ = r.readLine()
	return
}

func (r *Resp) Read() (Value, error) {
	ki, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch ki {
	case Sign_BULK_STRING:
		return r.ReadBulkStr()
	case Sign_ARRAY:
		return r.ReadArray()
	default:
		log.Printf("Unknown type: %v", ki)
		return Value{}, nil
	}
}

func (v *Value) Marshal() []byte {
	switch v.Ki {
	case STRING:
		return v.marshalString()
	case BULK_STRING:
		return v.marshalBulkStr()
	case ARRAY:
		return v.marshalArray()
	case ERROR:
		return v.marshalError()
	case NULL:
		return v.marshalNull()
	default:
		return []byte{}
	}
}

func putEndLine(rs []byte, pos int) {
	sz := len(rs)
	if pos >= sz {
		return
	}
	if pos < 0 {
		pos = max(0, sz-2)
	}
	if pos == sz-1 {
		rs[pos] = '\n'
		return
	}
	if pos < sz-1 {
		rs[pos] = '\r'
		rs[pos+1] = '\n'
	}
}

func addEndLine(rs []byte) []byte {
	return append(rs, '\r', '\n')
}

func (v *Value) marshalString() []byte {
	res := make([]byte, len(v.Str)+3)
	res[0] = Sign_STRING
	copy(res[1:], v.Str)
	putEndLine(res, -1)
	return res
}

func (v *Value) marshalBulkStr() []byte {
	size := len(v.BulkStr)
	sizeStr := strconv.Itoa(size)

	res := make([]byte, len(sizeStr)+len(v.BulkStr)+5)

	res[0] = Sign_BULK_STRING
	copy(res[1:], sizeStr)
	putEndLine(res, len(sizeStr)+1)
	copy(res[len(sizeStr)+3:], v.BulkStr)
	putEndLine(res, -1)
	return res
}

func (v *Value) marshalArray() []byte {
	size := len(v.Array)
	sizeStr := strconv.Itoa(size)

	res := make([]byte, len(sizeStr)+3)

	res[0] = Sign_ARRAY
	copy(res[1:], sizeStr)
	putEndLine(res, len(sizeStr)+1)

	for _, value := range v.Array {
		res = append(res, value.Marshal()...)
	}

	return res
}

func (v *Value) marshalNull() []byte {
	return []byte("$-1\r\n")
}

func (v *Value) marshalError() []byte {
	res := make([]byte, len(v.Str)+3)
	res[0] = Sign_ERROR
	copy(res[1:], v.Str)
	putEndLine(res, -1)
	return res
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {
	val := v.Marshal()
	_, err := w.writer.Write(val)
	return err
}
