package main

import (
	"context"
	"crypto/ed25519"
	"errors"
	"flag"
	"log"
	"net/http"
	"time"
	"encoding/hex"
	"encoding/json"

	"github.com/coder/websocket"
	"golang.org/x/time/rate"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dbPath := flag.String("db", "database.db", "path to sqlite file")
	fcmEnabled := flag.Bool("fcm", false, "Enable Firebase Cloud Messaging")

	flag.Parse()
	
	store, err := OpenStore(*dbPath)
	if err != nil {
		log.Fatal(err)
	}

	var pusher *Pusher

	if *fcmEnabled {
		pusher, err = NewPusher("./fcm.json")
		if err != nil {
			log.Fatalf("Pusher initialization failed: %v", err)
		}
	}

	hub := NewHub(store, pusher)
	http.HandleFunc("/ws", handleWS(hub))
	http.HandleFunc("/update", updateCheck())
	
	http.Handle("/apk/",
		http.StripPrefix(
			"/apk/",
			http.FileServer(http.Dir("./apk")),
		),
	)

	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()

		for range ticker.C {
			ttl := 7 * 24 * time.Hour

			if err := store.DeleteOlderThan(ttl); err != nil {
				log.Printf("messages cleanup error: %s", err)
			}

			if err := store.DeletePendingEventsOlderThan(ttl); err != nil {
				log.Printf("events cleanup error: %s", err)
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(24 * time.Hour)
		for range ticker.C {
			quota.Reset()
		}
	}()

	log.Fatal(http.ListenAndServe(*addr, nil))
}

func updateCheck() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		http.ServeFile(w, r, "./apk/update.json")
	}
}

func handleWS(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()

		ip := clientIP(r)

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		pubKey, err := authenticate(ctx, conn)
		if err != nil {
			return
		}
		cancel()

		limiter := rate.NewLimiter(5, 20)

		hub.Register(pubKey, conn)
		defer hub.Unregister(pubKey, conn)

		err = hub.SendPending(pubKey, conn)
		if err != nil {
			log.Printf("pending messages error: %s", err)
			conn.Close(websocket.StatusInternalError, "internal error")
			return
		}

		err = hub.SendPendingEvent(pubKey, conn)
		if err != nil {
			log.Printf("pending events error: %s", err)
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
					if !limiter.Allow() {
						conn.Close(websocket.StatusPolicyViolation, "rate limit exceeded")
						return
					}
					
					if len(m.To) != ed25519.PublicKeySize {
						conn.Close(websocket.StatusPolicyViolation, "wrong size recipient")
						return
					}

					if len(m.Blob) > 9*1024 {
						conn.Close(websocket.StatusPolicyViolation, "message too big")
						return
					}

					if !quota.Allow(ip, int64(len(m.Blob))+256) {
						log.Printf("quota exceeded: ip=%s", ip)
						conn.Close(websocket.StatusPolicyViolation, "quota exceeded")
						return
					}

					err := hub.Deliver(pubKey, m.To, m.Blob, m.SendAt, m.ClientMessageId)
					if err != nil {
						log.Printf("Deliver error: %s", err)
						conn.Close(websocket.StatusInternalError, "internal error")
						return
					}
					log.Printf("send: from=%x to=%x size=%d", pubKey[:4], m.To[:4], len(m.Blob))
				case ClientAck:
					err := hub.db.Delete(m.MessageId, pubKey)
					if err != nil {
						log.Printf("Ack delete error: %s", err)
						conn.Close(websocket.StatusInternalError, "internal error")
						return
					}
				case ClientPushToken:
					if len(m.Token) > 4096 || len(m.Token) == 0 {
						conn.Close(websocket.StatusPolicyViolation, "wrong size Token")
						return
					}
					err := hub.db.SaveToken(pubKey, m.Token)

					if err != nil {
						conn.Close(websocket.StatusInternalError, "internal error")
						log.Printf("[main.go] Save token error: %s", err)
						return
					}

					log.Printf("push token saved: %x", pubKey[:4])
				case ClientStatusRequest:
					online := hub.IsOnline(m.PubKey)

					result := ServerStatusResponce{
						Type: SERVER_STATUS_RESPONSE,
						PubKey: m.PubKey,
						Status: online,
					}

					data, err := Encode(result)
					if err != nil {
						log.Printf("Status encode error: %s", err)
						conn.Close(websocket.StatusInternalError, "internal error")
						return
					}

					statusCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
					defer cancel()

					err = conn.Write(statusCtx, websocket.MessageText, data)

					if err != nil {
						log.Printf("Status write error: %s", err)
						conn.Close(websocket.StatusInternalError, "internal error")
						return
					}
				case ClientDeleteMessage:
					if !limiter.Allow() {
						conn.Close(websocket.StatusPolicyViolation, "rate limit exceeded")
						return
					}

					requestId, err := GenerateRequestID()
					if err != nil {
						conn.Close(websocket.StatusInternalError, "internal error")
						return
					}

					if len(m.To) != ed25519.PublicKeySize {
						conn.Close(websocket.StatusPolicyViolation, "wrong size recipient")
						return
					}

					if len(m.Payload) == 0 || len(m.Payload) > 8192 {
						conn.Close(websocket.StatusPolicyViolation, "invalid payload size")
						return
					}

					var payload DeleteMessagePayload

					if err := json.Unmarshal(m.Payload, &payload); err != nil {
						conn.Close(websocket.StatusPolicyViolation, "invalid payload")
						return
					}

					if len(payload.ClientMessageIDs) == 0 ||
					len(payload.ClientMessageIDs) > 100 {
						conn.Close(websocket.StatusPolicyViolation, "invalid message ids")
						return
					}

					for _, id := range payload.ClientMessageIDs {
						if len(id) != 32 {
							conn.Close(websocket.StatusPolicyViolation, "invalid client message id")
							return
						}

						if _, err := hex.DecodeString(id); err != nil {
							conn.Close(websocket.StatusPolicyViolation, "invalid client message id")
							return
						}
					}


					if !quota.Allow(ip, int64(len(m.Payload))+256) {
						log.Printf("quota exceeded: ip=%s", ip)
						conn.Close(websocket.StatusPolicyViolation, "quota exceeded")
						return
					}

					pendingEvent := PendingEvent{
						recipient:  m.To,
						sender:     pubKey,
						eventType:  EventDeleteMessage,
						payload:    m.Payload,
						request_id: requestId,
						created_at: time.Now().Unix(),
					}

					err = hub.db.SavePendingEvent(pendingEvent)

					if err != nil {
						log.Printf("SavePendingEvent save pending event error: %s", err)
						conn.Close(websocket.StatusInternalError, "internal error")
						return 
					}

					err = hub.db.DeleteMessagesByClientIDs(
						m.To,
						pubKey,
						payload.ClientMessageIDs,
					)

					if err != nil {
						log.Printf("DeleteMessagesByClientIDs error: %s", err)
						conn.Close(websocket.StatusInternalError, "internal error")
						return
					}

					deleteMsgCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
					defer cancel()

					serverRequestDeleteMessage, err := Encode(ServerEvent{
						Type: SERVER_EVENT,
						RequestID: requestId,
						From: pubKey,
						EventType: EventDeleteMessage,
						Payload: m.Payload,
					})

					if err != nil {
						log.Printf("ClientDeleteMessage encode error")
						conn.Close(websocket.StatusInternalError, "internal error")
						return
					}

					toKey := m.To

					ok := hub.IsOnline(toKey)
					if ok {
						recipient, isFind := hub.getWsConnection(toKey)
						if isFind {
							recipient.Write(deleteMsgCtx, websocket.MessageText, serverRequestDeleteMessage)
						}
					}
				case ClientEventAck:
					if !limiter.Allow() {
						conn.Close(websocket.StatusPolicyViolation, "rate limit exceeded")
						return
					}

					if len(m.RequestID) != 32 {
						conn.Close(websocket.StatusPolicyViolation, "invalid request id")
						return
					}

					err := hub.db.DeletePendingEvent(m.RequestID, pubKey)
					if err != nil {
						log.Printf("DeletePendingEvent error: %s", err)
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