package processor

import (
	"sync"
	
	"app/resp"
)

type Keyval struct {
	data map[string]string
	lock sync.RWMutex
}

var keyvalLock = &sync.Mutex{}
var keyvalInstance *Keyval

func NewKeyval() *Keyval {
	if keyvalInstance == nil {
		keyvalLock.Lock()
		defer keyvalLock.Unlock()

		if keyvalInstance == nil {
			keyvalInstance = &Keyval{data: map[string]string{}, lock: sync.RWMutex{}}
		}
	}
	return keyvalInstance
}

func (s *Keyval) set(key, value string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.data[key] = value
}

func (s *Keyval) setAPI(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{Ki: resp.ERROR, Str: "Invalid request, SET expecting 2 arguments"}
	}
	s.set(args[0].BulkStr, args[1].BulkStr)
	return resp.Value{Ki: resp.STRING, Str: "OK"}
}

func (s *Keyval) get(key string) resp.Value {
	s.lock.RLock()
	val, ok := s.data[key]
	s.lock.RUnlock()
	if !ok {
		return resp.Value{Ki: resp.NULL}
	}

	return resp.Value{Ki: resp.BULK_STRING, BulkStr: val}
}

func (s *Keyval) getAPI(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Ki: resp.ERROR, Str: "Invalid request, GET expecting 1 arguments"}
	}
	return s.get(args[0].BulkStr)
}

func (s *Keyval) Process(cmd string, args []resp.Value) resp.Value {
	switch cmd {
	case "set":
		return s.setAPI(args)
	case "get":
		return s.getAPI(args)
	default:
		return InvalidCommand
	}
}
