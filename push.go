package main

import (
	"context"
	"time"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"
)

type Pusher struct {
	client *messaging.Client
}

func NewPusher(credsPath string) (*Pusher, error) {
    ctx := context.Background()

    opt := option.WithAuthCredentialsFile(
        option.ServiceAccount,
        credsPath,
    )

    app, err := firebase.NewApp(ctx, nil, opt)
    if err != nil {
        return nil, err
    }

    client, err := app.Messaging(ctx)
    if err != nil {
        return nil, err
    }

    return &Pusher{
        client: client,
    }, nil
}

func (p *Pusher) Send(ctx context.Context, token string) error {
	ttl := 3 * 24 * time.Hour
	
	msg := &messaging.Message{
		Token: token,
		Data: map[string]string{"type":"wake"},
		Android: &messaging.AndroidConfig{Priority: "high", TTL: &ttl},
	}

	_, err := p.client.Send(ctx, msg)

	if err != nil { return err }
	return nil
}