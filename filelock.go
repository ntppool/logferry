package main

import "sync"

type fileLock struct {
	m  map[string]bool
	mu sync.Mutex
}

func NewFileLock() *fileLock {
	return &fileLock{
		m: make(map[string]bool),
	}
}

func (fl *fileLock) IsLocked(name string) bool {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	_, ok := fl.m[name]
	return ok
}

func (fl *fileLock) Lock(name string) bool {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	if _, ok := fl.m[name]; ok {
		return false
	}
	fl.m[name] = true
	return true
}

func (fl *fileLock) Unlock(name string) {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	delete(fl.m, name)
}
