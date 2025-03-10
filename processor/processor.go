package processor

import (
	"app/resp"
)

type Processor interface {
	Process(cmd string, args []resp.Value) resp.Value
}

var InvalidCommand = resp.Value{Ki: resp.STRING, Str: "invalid command"}
