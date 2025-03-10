package processor

import "app/resp"

type Ping struct {
}

func NewPing() *Ping {
	return &Ping{}
}

func (p *Ping) Process(cmd string, v []resp.Value) resp.Value {
	resStr := "PONG"
	if v != nil && len(v) > 0 {
		resStr += ": " + v[0].BulkStr
	}
	return resp.Value{Ki: resp.STRING, Str: resStr}
}
