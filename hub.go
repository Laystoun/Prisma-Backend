package main

import (
	"sync"

	"github.com/coder/websocket"
)

type Hub struct {
	mutex 	sync.Mutex
	conns 	map[string]*websocket.Conn
	db 		*Store
}

func (h* Hub) Register(pubKey []byte, conn *websocket.Conn) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.conns[string(pubKey)] = conn
}

func (h* Hub) Unregister(pubKey []byte) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	delete(h.conns, string(pubKey))
}

func NewHub(db *Store) *Hub {
    return &Hub{
        conns: make(map[string]*websocket.Conn),
        db:    db,
    }
}