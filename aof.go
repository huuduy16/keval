package main

import (
	"app/resp"
	"bufio"
	"os"
	"sync"
	"time"
)

const SYNC_PERIOD = 2 * time.Second

type Aof struct {
	file   *os.File
	reader *bufio.Reader
	mu     *sync.Mutex
}

var aofLock = &sync.Mutex{}
var aofInstance *Aof

func NewAOF(filepath string) (*Aof, error) {
	if aofInstance == nil {
		aofLock.Lock()
		defer aofLock.Unlock()

		if aofInstance == nil {
			file, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0666)
			if err != nil {
				return nil, err
			}

			aofInstance = &Aof{
				file:   file,
				reader: bufio.NewReader(file),
				mu:     &sync.Mutex{},
			}
			aofInstance.syncFilePeriodically()
		}
	}
	return aofInstance, nil
}

func (a *Aof) syncFilePeriodically() {
	go func() {
		for {
			aofInstance.mu.Lock()
			_ = aofInstance.file.Sync()
			aofInstance.mu.Unlock()
			time.Sleep(SYNC_PERIOD)
		}
	}()
}

func (a *Aof) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.file.Close()
}

func (a *Aof) Persist(v resp.Value) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	_, err := a.file.Write(v.Marshal())
	return err
}

func (a *Aof) Recover(h *Handler) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	var wg sync.WaitGroup

	respReader := resp.NewResp(a.reader)

	for {
		v, err := respReader.Read()
		if err != nil {
			return err
		}

		wg.Wait()

		wg.Add(1)
		go func() {
			defer wg.Done()
			h.Handle(v)
		}()
	}
}
