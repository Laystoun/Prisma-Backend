// Тестовый клиент для Prisma-сервера.
// Запуск (сервер уже должен работать): go run ./testclient
package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const serverURL = "ws://localhost:8080/ws"

// Универсальное входящее сообщение. ID храним как есть (RawMessage),
// чтобы не зависеть от того, число это или строка: в ack вернём его же.
// Если в protocol.go поля называются иначе, поправь теги тут и ключи в map ниже.
type msg struct {
	Type  string          `json:"type"`
	Nonce []byte          `json:"nonce"`
	ID    json.RawMessage `json:"message_id"`
	From  []byte          `json:"from"`
	Blob  []byte          `json:"blob"`
}

// ---------- помощники ----------

func newKeys() (ed25519.PublicKey, ed25519.PrivateKey) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		panic(err)
	}
	return pub, priv
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}

func send(c *websocket.Conn, v any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return wsjson.Write(ctx, c, v)
}

func recv(c *websocket.Conn, timeout time.Duration) (msg, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var m msg
	err := wsjson.Read(ctx, c, &m)
	return m, err
}

func dial() (*websocket.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, serverURL, nil)
	return c, err
}

// hello: отправить hello и получить challenge, вернуть nonce.
func hello(c *websocket.Conn, pub ed25519.PublicKey) ([]byte, error) {
	if err := send(c, map[string]any{"type": "hello", "pub_key": []byte(pub)}); err != nil {
		return nil, err
	}
	m, err := recv(c, 3*time.Second)
	if err != nil {
		return nil, fmt.Errorf("ждали challenge: %w", err)
	}
	if m.Type != "challenge" || len(m.Nonce) == 0 {
		return nil, fmt.Errorf("ждали challenge, пришло %+v", m)
	}
	return m.Nonce, nil
}

// connect: полное рукопожатие, возвращает авторизованное соединение.
func connect(pub ed25519.PublicKey, priv ed25519.PrivateKey) (*websocket.Conn, error) {
	c, err := dial()
	if err != nil {
		return nil, err
	}
	nonce, err := hello(c, pub)
	if err != nil {
		c.CloseNow()
		return nil, err
	}
	sig := ed25519.Sign(priv, nonce)
	if err := send(c, map[string]any{"type": "auth", "sign": sig}); err != nil {
		c.CloseNow()
		return nil, err
	}
	m, err := recv(c, 3*time.Second)
	if err != nil {
		c.CloseNow()
		return nil, fmt.Errorf("ждали ok: %w", err)
	}
	if m.Type != "ok" {
		c.CloseNow()
		return nil, fmt.Errorf("ждали ok, пришло %+v", m)
	}
	return c, nil
}

// expectClose: сервер должен закрыть соединение с нужным кодом.
func expectClose(c *websocket.Conn, want websocket.StatusCode) error {
	_, err := recv(c, 3*time.Second)
	if err == nil {
		return fmt.Errorf("ждали закрытие соединения, а пришло сообщение")
	}
	if got := websocket.CloseStatus(err); got != want {
		return fmt.Errorf("код закрытия %d, ждали %d (err: %v)", got, want, err)
	}
	return nil
}

// sendOffline: from подключается, шлёт сообщение to и отключается.
func sendOffline(fromPub ed25519.PublicKey, fromPriv ed25519.PrivateKey, to ed25519.PublicKey, blob []byte) error {
	c, err := connect(fromPub, fromPriv)
	if err != nil {
		return err
	}
	defer c.CloseNow()
	err = send(c, map[string]any{"type": "send", "to": []byte(to), "blob": blob})
	time.Sleep(300 * time.Millisecond) // дать серверу время сохранить
	return err
}

// ---------- тесты ----------

type testCase struct {
	name string
	fn   func() error
}

var tests = []testCase{
	{"рукопожатие", func() error {
		pub, priv := newKeys()
		c, err := connect(pub, priv)
		if err != nil {
			return err
		}
		c.CloseNow()
		return nil
	}},

	{"отправка самому себе: send -> deliver -> ack", func() error {
		pub, priv := newKeys()
		c, err := connect(pub, priv)
		if err != nil {
			return err
		}
		defer c.CloseNow()

		blob := randBytes(64)
		if err := send(c, map[string]any{"type": "send", "to": []byte(pub), "blob": blob}); err != nil {
			return err
		}
		m, err := recv(c, 3*time.Second)
		if err != nil {
			return fmt.Errorf("ждали deliver: %w", err)
		}
		if m.Type != "deliver" {
			return fmt.Errorf("ждали deliver, пришло %+v", m)
		}
		if !bytes.Equal(m.From, pub) {
			return fmt.Errorf("поле from не совпало с отправителем")
		}
		if !bytes.Equal(m.Blob, blob) {
			return fmt.Errorf("blob изменился по дороге")
		}
		return send(c, map[string]any{"type": "ack", "message_id": m.ID})
	}},

	{"после ack сообщение удалено (не приходит повторно)", func() error {
		pub, priv := newKeys()
		c, err := connect(pub, priv)
		if err != nil {
			return err
		}
		if err := send(c, map[string]any{"type": "send", "to": []byte(pub), "blob": randBytes(16)}); err != nil {
			c.CloseNow()
			return err
		}
		m, err := recv(c, 3*time.Second)
		if err != nil {
			c.CloseNow()
			return err
		}
		if err := send(c, map[string]any{"type": "ack", "message_id": m.ID}); err != nil {
			c.CloseNow()
			return err
		}
		time.Sleep(300 * time.Millisecond)
		c.CloseNow()

		c2, err := connect(pub, priv)
		if err != nil {
			return err
		}
		defer c2.CloseNow()
		if m2, err := recv(c2, time.Second); err == nil {
			return fmt.Errorf("сообщение пришло повторно после ack: %+v", m2)
		}
		return nil
	}},

	{"офлайн-доставка и повтор без ack", func() error {
		aPub, aPriv := newKeys()
		bPub, bPriv := newKeys()
		blob := randBytes(32)
		if err := sendOffline(aPub, aPriv, bPub, blob); err != nil {
			return err
		}

		// B подключается и получает накопленное, но не подтверждает.
		c, err := connect(bPub, bPriv)
		if err != nil {
			return err
		}
		m1, err := recv(c, 3*time.Second)
		if err != nil {
			c.CloseNow()
			return fmt.Errorf("офлайн-сообщение не пришло: %w", err)
		}
		c.CloseNow()
		if m1.Type != "deliver" || !bytes.Equal(m1.From, aPub) || !bytes.Equal(m1.Blob, blob) {
			return fmt.Errorf("неверное офлайн-сообщение: %+v", m1)
		}
		time.Sleep(300 * time.Millisecond)

		// Без ack сервер обязан отдать то же самое ещё раз.
		c2, err := connect(bPub, bPriv)
		if err != nil {
			return err
		}
		defer c2.CloseNow()
		m2, err := recv(c2, 3*time.Second)
		if err != nil {
			return fmt.Errorf("повторной доставки без ack нет: %w", err)
		}
		if !bytes.Equal(m1.ID, m2.ID) {
			return fmt.Errorf("id изменился: %s и %s", m1.ID, m2.ID)
		}
		return send(c2, map[string]any{"type": "ack", "message_id": m2.ID})
	}},

	{"чужой ack не удаляет сообщение", func() error {
		aPub, aPriv := newKeys()
		bPub, bPriv := newKeys()
		cPub, cPriv := newKeys()
		if err := sendOffline(aPub, aPriv, bPub, randBytes(16)); err != nil {
			return err
		}

		cb, err := connect(bPub, bPriv)
		if err != nil {
			return err
		}
		m1, err := recv(cb, 3*time.Second)
		cb.CloseNow()
		if err != nil {
			return err
		}

		// Посторонний C пытается подтвердить чужое сообщение.
		cc, err := connect(cPub, cPriv)
		if err != nil {
			return err
		}
		_ = send(cc, map[string]any{"type": "ack", "message_id": m1.ID})
		time.Sleep(300 * time.Millisecond)
		cc.CloseNow()

		cb2, err := connect(bPub, bPriv)
		if err != nil {
			return err
		}
		defer cb2.CloseNow()
		m2, err := recv(cb2, 3*time.Second)
		if err != nil {
			return fmt.Errorf("сообщение B пропало после чужого ack: %w", err)
		}
		if !bytes.Equal(m1.ID, m2.ID) {
			return fmt.Errorf("пришло другое сообщение")
		}
		return send(cb2, map[string]any{"type": "ack", "message_id": m2.ID})
	}},

	{"подделка from: сервер берёт отправителя из соединения", func() error {
		aPub, aPriv := newKeys()
		bPub, bPriv := newKeys()
		fakePub, _ := newKeys()

		c, err := connect(aPub, aPriv)
		if err != nil {
			return err
		}
		_ = send(c, map[string]any{
			"type": "send", "to": []byte(bPub), "blob": randBytes(8),
			"from": []byte(fakePub), // лишнее поле, должно быть проигнорировано
		})
		time.Sleep(300 * time.Millisecond)
		c.CloseNow()

		cb, err := connect(bPub, bPriv)
		if err != nil {
			return err
		}
		defer cb.CloseNow()
		m, err := recv(cb, 3*time.Second)
		if err != nil {
			return err
		}
		if !bytes.Equal(m.From, aPub) {
			return fmt.Errorf("в from оказался не настоящий отправитель")
		}
		return send(cb, map[string]any{"type": "ack", "message_id": m.ID})
	}},

	{"неверная подпись не пускает", func() error {
		pub, _ := newKeys()
		c, err := dial()
		if err != nil {
			return err
		}
		defer c.CloseNow()
		if _, err := hello(c, pub); err != nil {
			return err
		}
		if err := send(c, map[string]any{"type": "auth", "sign": make([]byte, 64)}); err != nil {
			return err
		}
		m, err := recv(c, 3*time.Second)
		if err == nil && m.Type == "ok" {
			return fmt.Errorf("сервер принял неверную подпись")
		}
		return nil // error или закрытие соединения: оба варианта верны
	}},

	{"получатель неправильной длины -> PolicyViolation", func() error {
		pub, priv := newKeys()
		c, err := connect(pub, priv)
		if err != nil {
			return err
		}
		defer c.CloseNow()
		_ = send(c, map[string]any{"type": "send", "to": []byte{1, 2, 3, 4, 5}, "blob": randBytes(8)})
		return expectClose(c, websocket.StatusPolicyViolation)
	}},

	{"лишний hello после входа -> PolicyViolation", func() error {
		pub, priv := newKeys()
		c, err := connect(pub, priv)
		if err != nil {
			return err
		}
		defer c.CloseNow()
		_ = send(c, map[string]any{"type": "hello", "pub_key": []byte(pub)})
		return expectClose(c, websocket.StatusPolicyViolation)
	}},

	{"мусор вместо JSON -> PolicyViolation", func() error {
		pub, priv := newKeys()
		c, err := connect(pub, priv)
		if err != nil {
			return err
		}
		defer c.CloseNow()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = c.Write(ctx, websocket.MessageText, []byte("это не json"))
		return expectClose(c, websocket.StatusPolicyViolation)
	}},

	{"слишком большой blob -> MessageTooBig", func() error {
		pub, priv := newKeys()
		c, err := connect(pub, priv)
		if err != nil {
			return err
		}
		defer c.CloseNow()
		_ = send(c, map[string]any{"type": "send", "to": []byte(pub), "blob": randBytes(200 * 1024)})
		// Сервер может закрыть сокет раньше, чем клиент дольёт данные: тогда вместо
		// кода 1009 придёт обрыв (RST, код -1). Оба исхода значат «не принято».
		m, err := recv(c, 3*time.Second)
		if err == nil {
			return fmt.Errorf("сервер принял слишком большое сообщение: %s", m.Type)
		}
		if st := websocket.CloseStatus(err); st != websocket.StatusMessageTooBig && st != -1 {
			return fmt.Errorf("неожиданный код закрытия %d: %v", st, err)
		}
		return nil
	}},
}

func main() {
	failed := 0
	for _, t := range tests {
		if err := t.fn(); err != nil {
			fmt.Printf("FAIL  %s\n      %v\n", t.name, err)
			failed++
		} else {
			fmt.Printf("ok    %s\n", t.name)
		}
	}
	fmt.Printf("\nпровалено: %d из %d\n", failed, len(tests))
	if failed > 0 {
		os.Exit(1)
	}
}