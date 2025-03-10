package main

import (
	"app/resp"
	"log"
	"slices"
	"strings"

	"app/processor"
)

type Handler struct {
	router map[string]processor.Processor
}

var UpsertCommands = []string{"set", "hset"}

func NewHandler() *Handler {
	return &Handler{
		router: map[string]processor.Processor{
			"ping":    processor.NewPing(),
			"get":     processor.NewKeyval(),
			"set":     processor.NewKeyval(),
			"hset":    processor.NewHKeyval(),
			"hget":    processor.NewHKeyval(),
			"hgetall": processor.NewHKeyval(),
		},
	}
}

func (h *Handler) Register(name string, proc processor.Processor) {
	h.router[name] = proc
}

func (h *Handler) IsUpsertCommand(cmd string) bool {
	return slices.Contains(UpsertCommands, strings.ToLower(cmd))
}

func (h *Handler) ParseQuery(inp resp.Value) (string, []resp.Value, resp.Value) {
	log.Printf("Resp: %#v", inp)
	if inp.Ki != resp.ARRAY {
		return "", nil, resp.Value{Ki: resp.ERROR, Str: "Invalid request, expecting ARRAY"}
	}
	if inp.Array == nil || len(inp.Array) == 0 {
		return "", nil, resp.Value{Ki: resp.ERROR, Str: "Invalid request, expecting array length > 0"}
	}
	if inp.Array[0].Ki != resp.BULK_STRING {
		return "", nil, resp.Value{Ki: resp.ERROR, Str: "Invalid command, expecting BULK_STRING"}
	}

	cmd := strings.ToLower(inp.Array[0].BulkStr)
	if _, ok := h.router[cmd]; !ok {
		return cmd, nil, resp.Value{Ki: resp.ERROR, Str: "Invalid command: " + cmd}
	}
	return cmd, inp.Array[1:], resp.Value{Ki: resp.NULL}
}

func (h *Handler) Process(cmd string, args []resp.Value) resp.Value {
	if val, ok := h.router[cmd]; !ok {
		return resp.Value{Ki: resp.ERROR, Str: "Invalid command: " + cmd}
	} else {
		return val.Process(cmd, args)
	}
}

func (h *Handler) Handle(inp resp.Value) {
	cmd, args, queryErr := h.ParseQuery(inp)
	if queryErr.Ki != resp.NULL {
		return
	}
	if h.IsUpsertCommand(cmd) {
		_ = h.Process(cmd, args)
	}
}
