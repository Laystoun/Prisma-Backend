package main

import (
	"context"
	"log"
	"sync"
	"time"

	"firebase.google.com/go/v4/messaging"
	"github.com/coder/websocket"
)

/*struct for conns and deliver msgs*/
type Hub struct {
	mutex 	sync.RWMutex
	conns 	map[string]*websocket.Conn
	db 		*Store
	pusher  *Pusher
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

func (h *Hub) IsOnline(pubKey []byte) bool {
    h.mutex.RLock()
    defer h.mutex.RUnlock()

    _, ok := h.conns[string(pubKey)]
    return ok
}

func (h* Hub) Wake(to []byte) {
	if h.pusher == nil {
		return
	}
	token, err := h.db.GetToken(to)
	if err != nil {
		log.Printf("[hub.go] Wake() error - GetToken return err: %s", err)
		return
	}

	if h.pusher == nil || token == "" { return }

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	
	go func(ctx context.Context) {
		err := h.pusher.Send(ctx, token)
		defer cancel()
		if err != nil {
			if messaging.IsUnregistered(err) {
				err := h.db.DeleteToken(token)
				if err != nil { log.Printf("[hub.go] Wake() error delete token: %s", err) }
				return
			}
			log.Printf("FCM Send error: %v", err)
		}
	}(ctx)
}

func NewHub(db *Store, pusher *Pusher) *Hub {
    return &Hub{
        conns: make(map[string]*websocket.Conn),
        db:    db,
		pusher: pusher,
    }
}

func (h *Hub) Deliver(from, to, blob []byte, send_at int64) error {
	fromSender, err := h.db.CountMessages(to, from)
	if err != nil {
		return err
	}
	if fromSender >= 50 {
		log.Printf("limit per sender: from=%x to=%x", from[:4], to[:4])
		return nil
	}

	total, err := h.db.CountInbox(to)
	if err != nil {
		return err
	}
	if total >= 500 {
		log.Printf("inbox full: to=%x", to[:4])
		return nil
	}

	id, err := h.db.Save(to, from, blob, send_at)
	if err != nil {
		return err
	}

	h.mutex.Lock()
	conn, ok := h.conns[string(to)]
	h.mutex.Unlock()

	if !ok {
		h.Wake(to)
		return nil
	}

	if err := sendDeliver(conn, id, from, blob, send_at); err != nil {
		log.Printf("deliver to online client failed: %s", err)
		h.Wake(to)
	}
	return nil
}

func sendDeliver(conn *websocket.Conn, msgId int64, from, blob []byte, send_at int64) error {
	srvrDeliver := ServerDeliver{
		Type: SERVER_DELIVER,
		MessageId: msgId,
		From: from,
		Blob: blob,
		SendAt: send_at,
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
		err := sendDeliver(conn, message.ID, message.From, message.Blob, message.CreatedAt)
		if err != nil {
			return err
		}
	}

	return nil
}