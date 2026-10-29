package main

import (
	"context"
	"crypto/ed25519"
	"errors"
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

// verbose включается флагом -LOGS
var verbose bool

// logf печатает лог только если запущено с -LOGS
func logf(format string, args ...any) {
	if verbose {
		log.Printf(format, args...)
	}
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dbPath := flag.String("db", "database.db", "path to sqlite file")
	logs := flag.Bool("LOGS", false, "enable verbose logs")
	flag.Parse()
	verbose = *logs

	logf("[main] start: addr=%s db=%s", *addr, *dbPath)

	store, err := OpenStore(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	logf("[main] store opened")

	hub := NewHub(store)
	logf("[main] hub created")
	http.HandleFunc("/ws", handleWS(hub))

	go func() {
		ticker := time.NewTicker(time.Hour)
		for range ticker.C {
			logf("[cleanup] deleting messages older than 30 days")
			err := store.DeleteOlderThan(30 * 24 * time.Hour)
			if err != nil {
				logf("cleanup error: %s", err)
				continue
			}
			logf("[cleanup] done")
		}
	}()

	logf("[main] listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}

func handleWS(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logf("[ws] incoming connection from %s", r.RemoteAddr)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			logf("[ws] accept error from %s: %s", r.RemoteAddr, err)
			return
		}
		defer conn.CloseNow()
		start := time.Now()
		defer func() {
			logf("[ws] connection closed: remote=%s lived=%s", r.RemoteAddr, time.Since(start))
		}()
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		pubKey, err := authenticate(ctx, conn)
		if err != nil {
			logf("[ws] auth failed: remote=%s err=%s", r.RemoteAddr, err)
			return
		}
		cancel()
		logf("[ws] auth ok: remote=%s key=%x", r.RemoteAddr, pubKey[:4])

		hub.Register(pubKey, conn)
		logf("[ws] registered: key=%x", pubKey[:4])
		defer logf("[ws] unregistered: key=%x", pubKey[:4])
		defer hub.Unregister(pubKey, conn)

		logf("[ws] sending pending messages: key=%x", pubKey[:4])
		err = hub.SendPending(pubKey, conn)

		if err != nil {
			logf("pending messages error: %s", err)
			conn.Close(websocket.StatusInternalError, "internal error")
			return
		}
		logf("[ws] pending messages sent: key=%x", pubKey[:4])

		for {
			_, data, err := conn.Read(r.Context())
			if err != nil {
				logf("[ws] read error: key=%x err=%s", pubKey[:4], err)
				return
			}
			logf("[ws] got %d bytes from key=%x", len(data), pubKey[:4])

			msg, err := Decode(data)
			if err != nil {
				logf("[ws] decode error: key=%x err=%s", pubKey[:4], err)
				conn.Close(websocket.StatusPolicyViolation, "bad message")
				return
			}

			switch m := msg.(type) {
			case ClientSend:
				logf("[ws] ClientSend: from=%x blob=%d bytes", pubKey[:4], len(m.Blob))
				if len(m.To) != ed25519.PublicKeySize {
					logf("[ws] wrong recipient size: %d", len(m.To))
					conn.Close(websocket.StatusPolicyViolation, "wrong size recipient")
					return
				}
				err := hub.Deliver(pubKey, m.To, m.Blob)
				if err != nil {
					logf("Deliver error: %s", err)
					conn.Close(websocket.StatusInternalError, "internal error")
					return
				}
				logf("send: from=%x to=%x size=%d", pubKey[:4], m.To[:4], len(m.Blob))
			case ClientAck:
				logf("[ws] ClientAck: key=%x messageId=%v", pubKey[:4], m.MessageId)
				err := hub.db.Delete(m.MessageId, pubKey)
				if err != nil {
					logf("Ack delete error: %s", err)
					conn.Close(websocket.StatusInternalError, "internal error")
					return
				}
				logf("[ws] message deleted after ack: messageId=%v", m.MessageId)
			default:
				logf("[ws] unexpected message type %T from key=%x", msg, pubKey[:4])
				conn.Close(websocket.StatusPolicyViolation, "unexpected message")
				return
			}
		}
	}
}

func reject(conn *websocket.Conn, code websocket.StatusCode, reason string) error {
	logf("[reject] code=%d reason=%s", code, reason)
	conn.Close(code, reason)
	return errors.New(reason)
}

func authenticate(ctx context.Context, conn *websocket.Conn) ([]byte, error) {
	// client hello
	logf("[auth] waiting for client hello")
	_, data, err := conn.Read(ctx)
	if err != nil {
		logf("[auth] read hello error: %s", err)
		return nil, err
	}
	decoded, err := Decode(data)
	if err != nil {
		return nil, reject(conn, websocket.StatusPolicyViolation, "bad message")
	}
	hello, ok := decoded.(ClientHello)
	if !ok {
		return nil, reject(conn, websocket.StatusPolicyViolation, "expected hello")
	}
	pubKey := hello.PubKey
	logf("[auth] hello received: pubKey size=%d", len(pubKey))
	if len(pubKey) != ed25519.PublicKeySize {
		return nil, reject(conn, websocket.StatusPolicyViolation, "wrong size pubKey")
	}

	// server challenge
	nonce, err := NewChallenge()
	if err != nil {
		logf("NewChallenge failed: %s", err)
		return nil, reject(conn, websocket.StatusInternalError, "internal error")
	}
	challengeJson, err := Encode(ServerChallenge{Type: SERVER_CHALLENGE, Nonce: nonce})
	if err != nil {
		logf("Encode challenge failed: %s", err)
		return nil, reject(conn, websocket.StatusInternalError, "internal error")
	}
	if err := conn.Write(ctx, websocket.MessageText, challengeJson); err != nil {
		logf("[auth] write challenge error: %s", err)
		return nil, err
	}
	logf("[auth] challenge sent: key=%x", pubKey[:4])

	// client auth
	_, data, err = conn.Read(ctx)
	if err != nil {
		logf("[auth] read auth error: %s", err)
		return nil, err
	}
	decoded, err = Decode(data)
	if err != nil {
		return nil, reject(conn, websocket.StatusPolicyViolation, "bad message")
	}
	auth, ok := decoded.(ClientAuth)
	if !ok {
		return nil, reject(conn, websocket.StatusPolicyViolation, "expected auth")
	}
	logf("[auth] auth received, verifying signature: key=%x", pubKey[:4])
	if !VerifySignature(pubKey, nonce, auth.Sign) {
		return nil, reject(conn, websocket.StatusPolicyViolation, "invalid signature")
	}
	logf("[auth] signature valid: key=%x", pubKey[:4])

	// ok
	okJson, err := Encode(ServerOk{Type: SERVER_OK})
	if err != nil {
		logf("Encode ok failed: %s", err)
		return nil, reject(conn, websocket.StatusInternalError, "internal error")
	}
	if err := conn.Write(ctx, websocket.MessageText, okJson); err != nil {
		logf("[auth] write ok error: %s", err)
		return nil, err
	}
	logf("[auth] ok sent: key=%x", pubKey[:4])

	return pubKey, nil
}