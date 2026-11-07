package main

import (
	"database/sql"
	"time"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

/* Client Service consts */
const (
	EventDeleteMessage = "delete_message"
)

type Store struct{ db *sql.DB }
type Message struct {
	ID        		int64
	Blob      		[]byte
	From 	  		[]byte
	CreatedAt 		int64
	ClientMessageId string
}

type PendingEvent struct {
	Id			int64
	recipient	[]byte
	sender 		[]byte
	eventType	string
	payload		[]byte
	request_id 	string
	created_at 	int64
}

func OpenStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	_, err = db.Exec(`
		PRAGMA journal_mode=WAL;
		CREATE TABLE IF NOT EXISTS messages (
			id 			INTEGER PRIMARY KEY AUTOINCREMENT,
			recipient	BLOB NOT NULL,
			sender 		BLOB NOT NULL,
			blob		BLOB NOT NULL,
			cl_msg_id   TEXT NOT NULL DEFAULT '',
			created_at	INTEGER NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_messages_recipient ON messages(recipient);

		CREATE TABLE IF NOT EXISTS push_tokens (
			pubKey BLOB PRIMARY KEY,
			token TEXT NOT NULL,
			updated_at INTEGER
		);

		CREATE TABLE IF NOT EXISTS pending_events (
			id          INTEGER PRIMARY KEY AUTOINCREMENT,
			recipient   BLOB NOT NULL,
			sender      BLOB NOT NULL,
			type        TEXT NOT NULL,
			payload     BLOB NOT NULL,
			request_id  TEXT NOT NULL,
			created_at  INTEGER NOT NULL,

			UNIQUE(sender, request_id)
		);

		CREATE INDEX IF NOT EXISTS idx_pending_events_recipient
		ON pending_events(recipient);
	`)

	if err != nil {
		db.Close()
		return nil, err
	}

	return &Store{db: db}, nil
}

func (s *Store) SaveToken(pubKey []byte, token string) error {
	_, err := s.db.Exec(`
		INSERT INTO push_tokens (pubKey, token, updated_at) 
		VALUES (?, ?, ?)
		ON CONFLICT (pubKey) 
		DO UPDATE SET 
			token = EXCLUDED.token, 
			updated_at = EXCLUDED.updated_at
	`, pubKey, token, time.Now().Unix())
	
	if err != nil { 
		return err 
	}

	return nil
}

func (s *Store) GetToken(pubKey []byte) (string, error) {
	row, err := s.db.Query(`
		SELECT token FROM push_tokens WHERE pubKey = ?
	`, (pubKey))
	if err != nil { return "", err }
	defer row.Close()

	var token string
	if row.Next() {
		err = row.Scan(&token)
		if err != nil {
			return "", err
		}

		return token, nil
	}

	return "", nil
}

func (s *Store) DeleteToken(token string) error {
	_, err := s.db.Exec(
		"DELETE FROM push_tokens WHERE token = ?", token,
	)

	if err != nil { return err }
	return nil
}

func (s *Store) Save(recipient, senderPubKey, blob []byte, send_at int64, client_message_id string) (int64, error) {
	res, err := s.db.Exec(`
		INSERT INTO messages (recipient, sender, blob, cl_msg_id, created_at) VALUES (?, ?, ?, ?, ?)
	`, recipient, senderPubKey, blob, client_message_id, send_at)
	if err != nil {
		return 0, err
	}
	msgId, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return msgId, nil
}

func (s *Store) Pending(recipient []byte) ([]Message, error) {
	msgs := make([]Message, 0)

	rows, err := s.db.Query(`
		SELECT id, sender, blob, cl_msg_id, created_at FROM messages WHERE recipient = ? ORDER BY id
	`, recipient)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var m Message
		err := rows.Scan(&m.ID, &m.From, &m.Blob, &m.ClientMessageId, &m.CreatedAt)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}

	return msgs, rows.Err()
}

func (s *Store) Delete(id int64, recipient []byte) error {
	_, err := s.db.Exec(`
		DELETE FROM messages WHERE id = ? AND recipient = ?
	`, id, recipient)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) DeleteOlderThan(ttl time.Duration) error {
	cutoff := time.Now().Add(-ttl).Unix()
	_, err := s.db.Exec(`
		DELETE FROM messages WHERE created_at < ?
	`, 	cutoff)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) DeletePendingEventsOlderThan(ttl time.Duration) error {
    cutoff := time.Now().Add(-ttl).Unix()

    _, err := s.db.Exec(`
        DELETE FROM pending_events
        WHERE created_at < ?
    `, cutoff)

    return err
}

func (s *Store) Close() error { 
	err := s.db.Close()
	if err != nil { return err }
	return nil
}

func (s *Store) CountMessages(pubKey []byte, sender []byte) (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM messages WHERE recipient = ? AND sender = ?`, pubKey, sender).Scan(&count)
	if err != nil { return 0, err }
	return count, nil
}

func (s *Store) CountInbox(recipient []byte) (int, error) {
	var count int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM messages WHERE recipient = ?`, recipient,
	).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Store) SavePendingEvent(event PendingEvent) (error) {
	_, err := s.db.Exec(`
		INSERT INTO pending_events (recipient, sender, type, payload, request_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, event.recipient, event.sender, event.eventType, event.payload, event.request_id, event.created_at)

	if err != nil {
		return err
	}
	return nil
}

func (s *Store) GetPendingEvents(recipient []byte) ([]PendingEvent, error) {
	events := make([]PendingEvent, 0)

	rows, err := s.db.Query(`
		SELECT * FROM pending_events WHERE recipient = ? ORDER BY created_at
	`, recipient)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var ev PendingEvent
		err := rows.Scan(&ev.Id, &ev.recipient, &ev.sender, &ev.eventType, &ev.payload, &ev.request_id, &ev.created_at)
		if err != nil { return nil, err }
		events = append(events, ev)
	}

	return events, rows.Err()
}

func (s *Store) DeletePendingEvent(requestId string, pubKey []byte) (error) {
	_, err := s.db.Exec(`
		DELETE FROM pending_events WHERE recipient = ? AND request_id = ?
	`, pubKey, requestId)

	if err != nil {
		return err
	}

	return nil
}

func (s *Store) DeleteMessagesByClientIDs(
    recipient []byte,
    sender []byte,
    ids []string,
) error {
    if len(ids) == 0 {
        return nil
    }

    placeholders := make([]string, len(ids))
    args := make([]any, 0, len(ids)+2)

    args = append(args, recipient, sender)

    for i, id := range ids {
        placeholders[i] = "?"
        args = append(args, id)
    }

    query := fmt.Sprintf(`
        DELETE FROM messages
        WHERE recipient = ?
        AND sender = ?
        AND cl_msg_id IN (%s)
    `, strings.Join(placeholders, ","))

    _, err := s.db.Exec(query, args...)
    return err
}