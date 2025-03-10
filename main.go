package main

import (
	"app/resp"
	"log"
	"net"
)

func main() {
	l, serverErr := net.Listen("tcp", ":6379")
	if serverErr != nil {
		log.Fatal(serverErr)
	}
	defer func(l net.Listener) { _ = l.Close() }(l)

	var inp, ou resp.Value
	h := NewHandler()

	aof, persistErr := NewAOF("data/backup0")
	if persistErr != nil {
		log.Fatal(persistErr)
	}
	defer func() { _ = aof.Close() }()

	_ = aof.Recover(h)

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}

		respReader := resp.NewResp(conn)
		writer := resp.NewWriter(conn)
		for {
			inp, err = respReader.Read()
			if err != nil {
				log.Printf("Resp.Read error: %v", err)
				break
			}
			log.Printf("Resp: %#v", inp)

			cmd, args, queryErr := h.ParseQuery(inp)
			if queryErr.Ki != resp.NULL {
				_ = writer.Write(queryErr)
				continue
			}

			ou = h.Process(cmd, args)

			if h.IsUpsertCommand(cmd) {
				_ = aof.Persist(inp)
			}

			if writer.Write(ou) != nil {
				log.Printf("Writer.Write error: %v", err)
				break
			}
		}

		_ = conn.Close()
	}
}
