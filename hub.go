package main

import (
	"context"
	"log"
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

func (h* Hub) Unregister(pubKey []byte, conn *websocket.Conn) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	if h.conns[string(pubKey)] == conn {
		delete(h.conns, string(pubKey))
	}
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

	if err := sendDeliver(conn, id, from, blob); err != nil {
		log.Printf("deliver to online client failed: %s", err)
	}
	return nil
}

func sendDeliver(conn *websocket.Conn, msgId int64, from, blob []byte) error {
	srvrDeliver := ServerDeliver{
		Type: SERVER_DELIVER,
		MessageId: msgId,
		From: from,
		Blob: blob,
	}

	srvrDeliverJson, err := Encode(srvrDeliver)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return conn.Write(ctx, websocket.MessageText, srvrDeliverJson)
}

func (h* Hub) SendPending(pubKey []byte, conn *websocket.Conn) error {
	msgs, err := h.db.Pending(pubKey)
	if err != nil {
		return err
	}

	for _, message := range msgs {
		err := sendDeliver(conn, message.ID, message.From, message.Blob)
		if err != nil {
			return err
		}
	}

	return nil
}