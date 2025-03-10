package processor

import (
	"sync"
	
	"app/resp"
)

type HKeyval struct {
	data map[string]map[string]string
	lock sync.RWMutex
}

var hKeyvalLock = &sync.Mutex{}
var hKeyvalInstance *HKeyval

func NewHKeyval() *HKeyval {
	if hKeyvalInstance == nil {
		hKeyvalLock.Lock()
		defer hKeyvalLock.Unlock()

		if hKeyvalInstance == nil {
			hKeyvalInstance = &HKeyval{data: map[string]map[string]string{}, lock: sync.RWMutex{}}
		}
	}
	return hKeyvalInstance
}

func (s *HKeyval) hSet(key, field, value string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.data[key]; !ok {
		s.data[key] = make(map[string]string)
	}
	s.data[key][field] = value
}

func (s *HKeyval) hSetAPI(args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.Value{Ki: resp.ERROR, Str: "Invalid request, HSET expecting 3 arguments"}
	}
	s.hSet(args[0].BulkStr, args[1].BulkStr, args[2].BulkStr)
	return resp.Value{Ki: resp.STRING, Str: "OK"}
}

func (s *HKeyval) hGet(key, field string) resp.Value {
	var val string
	var ok bool
	s.lock.RLock()
	if _, ok = s.data[key]; !ok {
		s.lock.RUnlock()
		return resp.Value{Ki: resp.NULL}
	}
	if val, ok = s.data[key][field]; !ok {
		s.lock.RUnlock()
		return resp.Value{Ki: resp.NULL}
	}

	s.lock.RUnlock()
	return resp.Value{Ki: resp.BULK_STRING, BulkStr: val}
}

func (s *HKeyval) hGetAPI(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{Ki: resp.ERROR, Str: "Invalid request, HGET expecting 2 arguments"}
	}
	return s.hGet(args[0].BulkStr, args[1].BulkStr)
}

func (s *HKeyval) hGetAll(key string) resp.Value {
	s.lock.RLock()

	if _, ok := s.data[key]; !ok {
		s.lock.RUnlock()
		return resp.Value{Ki: resp.NULL}
	}

	rsFields := make([]resp.Value, len(s.data[key])*2)
	i := 0
	for field, val := range s.data[key] {
		rsFields[i] = resp.Value{Ki: resp.STRING, Str: field}
		i++
		rsFields[i] = resp.Value{Ki: resp.STRING, Str: val}
		i++
	}

	s.lock.RUnlock()

	return resp.Value{Ki: resp.ARRAY, Array: rsFields}
}

func (s *HKeyval) hGetAllAPI(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Ki: resp.ERROR, Str: "Invalid request, HGETALL expecting 1 arguments"}
	}
	return s.hGetAll(args[0].BulkStr)
}

func (s *HKeyval) Process(cmd string, args []resp.Value) resp.Value {
	switch cmd {
	case "hset":
		return s.hSetAPI(args)
	case "hget":
		return s.hGetAPI(args)
	case "hgetall":
		return s.hGetAllAPI(args)
	default:
		return InvalidCommand
	}
}
