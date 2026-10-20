package main

import (
	"database/sql"
	"time"

	_ "modernc.org/sqlite"
)

/*
type Store struct { db *sql.DB }
type Message struct { ID int64; Blob []byte; CreatedAt int64 }

func OpenStore(path string) (*Store, error)
func (s *Store) Save(recipient, blob []byte) error
func (s *Store) Pending(recipient []byte) ([]Message, error)
func (s *Store) Delete(id int64, recipient []byte) error
func (s *Store) DeleteOlderThan(ttl time.Duration) error
func (s *Store) Close() error
*/

type Store struct{ db *sql.DB }
type Message struct {
	ID        int64
	Blob      []byte
	CreatedAt int64
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
			blob		BLOB NOT NULL,
			created_at	INTEGER NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_messages_recipient ON messages(recipient);
	`)

	if err != nil {
		db.Close()
		return nil, err
	}

	return &Store{db: db}, nil
}

func (s *Store) Save(recipient, blob []byte) error {
	_, err := s.db.Exec(`
		INSERT INTO messages (recipient, blob, created_at) VALUES (?, ?, ?)
	`, recipient, blob, time.Now().Unix())
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) Pending(recipient []byte) ([]Message, error) {
	msgs := make([]Message, 0)

	rows, err := s.db.Query(`
		SELECT id, blob, created_at FROM messages WHERE recipient = ? ORDER BY id
	`, recipient)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var m Message
		err := rows.Scan(&m.ID, &m.Blob, &m.CreatedAt)
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

func (s *Store) Close() error { 
	err := s.db.Close()
	if err != nil { return err }
	return nil
}