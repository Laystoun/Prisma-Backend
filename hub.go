package main

import (
	"context"
	"sync"
	"time"

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

func (h* Hub) Deliver(from, to, blob []byte) error {
	id, err := h.db.Save(to, from, blob)
	if err != nil {
		return err
	}

	h.mutex.Lock()
	conn, ok := h.conns[string(to)]
	h.mutex.Unlock()

	if !ok {
		return nil
	}

	srvrDeliver := ServerDeliver{
		Type: SERVER_DELIVER,
		MessageId: id,
		From: from,
		Blob: blob,
	}

	decodeDeliverBytes, err := Encode(srvrDeliver)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	conn.Write(ctx, websocket.MessageText, decodeDeliverBytes)
	return nil
}