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

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dbPath := flag.String("db", "database.db", "path to sqlite file")
	flag.Parse()
	
	store, err := OpenStore(*dbPath)
	if err != nil {
		log.Fatal(err)
	}

	hub := NewHub(store)
	http.HandleFunc("/ws", handleWS(hub))

	go func() {
		ticker := time.NewTicker(time.Hour)
		for range ticker.C {
			err := store.DeleteOlderThan(30 * 24 * time.Hour)
			if err != nil {
				log.Printf("cleanup error: %s", err)
			}
		}
	}()

	log.Fatal(http.ListenAndServe(*addr, nil))
}

func handleWS(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		pubKey, err := authenticate(ctx, conn)
		if err != nil {
			return
		}
		cancel()

		hub.Register(pubKey, conn)
		defer hub.Unregister(pubKey, conn)
		err = hub.SendPending(pubKey, conn)
		
		if err != nil {
			log.Printf("pending messages error: %s", err)
			conn.Close(websocket.StatusInternalError, "internal error")
			return
		}

		for {
			_, data, err := conn.Read(r.Context())
			if err != nil {
				return
			}

			msg, err := Decode(data)
			if err != nil {
				conn.Close(websocket.StatusPolicyViolation, "bad message")
				return
			}

			switch m := msg.(type) {
				case ClientSend:
					if len(m.To) != ed25519.PublicKeySize {
						conn.Close(websocket.StatusPolicyViolation, "wrong size recipient")
						return
					}
					err := hub.Deliver(pubKey, m.To, m.Blob)
					if err != nil {
						log.Printf("Deliver error: %s", err)
						conn.Close(websocket.StatusInternalError, "internal error")
						return
					}
				case ClientAck:
					err := hub.db.Delete(m.MessageId, pubKey)
					if err != nil {
						log.Printf("Ack delete error: %s", err)
						conn.Close(websocket.StatusInternalError, "internal error")
						return
					}
				default:
					conn.Close(websocket.StatusPolicyViolation, "unexpected message")
					return
			}
		}
	}
}

func reject(conn *websocket.Conn, code websocket.StatusCode, reason string) error {
    conn.Close(code, reason)
    return errors.New(reason)
}

func authenticate(ctx context.Context, conn *websocket.Conn) ([]byte, error) {
    // client hello
    _, data, err := conn.Read(ctx)
    if err != nil {
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
    if len(pubKey) != ed25519.PublicKeySize {
        return nil, reject(conn, websocket.StatusPolicyViolation, "wrong size pubKey")
    }

    // server challenge
    nonce, err := NewChallenge()
    if err != nil {
        log.Printf("NewChallenge failed: %s", err)
        return nil, reject(conn, websocket.StatusInternalError, "internal error")
    }
    challengeJson, err := Encode(ServerChallenge{Type: SERVER_CHALLENGE, Nonce: nonce})
    if err != nil {
        log.Printf("Encode challenge failed: %s", err)
        return nil, reject(conn, websocket.StatusInternalError, "internal error")
    }
    if err := conn.Write(ctx, websocket.MessageText, challengeJson); err != nil {
        return nil, err
    }

    // client auth
    _, data, err = conn.Read(ctx)
    if err != nil {
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
    if !VerifySignature(pubKey, nonce, auth.Sign) {
        return nil, reject(conn, websocket.StatusPolicyViolation, "invalid signature")
    }

    // ok
    okJson, err := Encode(ServerOk{Type: SERVER_OK})
    if err != nil {
        log.Printf("Encode ok failed: %s", err)
        return nil, reject(conn, websocket.StatusInternalError, "internal error")
    }
    if err := conn.Write(ctx, websocket.MessageText, okJson); err != nil {
        return nil, err
    }

    return pubKey, nil
}