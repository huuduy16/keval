package main

import (
	"io"
	"log"
	"net"
)

func main() {
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatal(err)
	}
	defer func(l net.Listener) {
		_ = l.Close()
	}(l)

	conn, err := l.Accept()
	if err != nil {
		log.Fatal(err)
	}

	buff := make([]byte, 1024)
	for {
		_, err := conn.Read(buff)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		_, _ = conn.Write([]byte("+OK\r\n"))
	}
}
