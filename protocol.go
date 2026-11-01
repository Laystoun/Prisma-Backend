package main

import (
	"encoding/json"
	"fmt"
)

/*
от клиента: hello, auth, send, ack
от сервера: challenge, ok, error, deliver
*/

const CLIENT_HELLO = "hello"
const CLIENT_AUTH = "auth"
const CLIENT_SEND = "send"
const CLIENT_ACK = "ack"
const CLIENT_PUSH_TOKEN = "push_token"

const SERVER_CHALLENGE = "challenge"
const SERVER_OK = "ok"
const SERVER_ERROR = "error"
const SERVER_DELIVER = "deliver"

type ClientHello struct { 
	Type string 	`json:"type"`
	PubKey []byte 	`json:"pub_key"`
}

type ClientAuth struct {
	Type string 	`json:"type"`
	Sign []byte		`json:"sign"`
}

type ClientSend struct {
	Type string 	`json:"type"`
	To []byte 		`json:"to"`
	Blob []byte 	`json:"blob"`
}

/*
{"type":"send", "to":"q+H1GQmZaRForNbmLH1ydq2PzB9aFU4OuYnoMrPQ8pQ=", "blob":"Hello"}
*/

type ClientAck struct {
	Type string 	`json:"type"`
	MessageId int64 `json:"message_id"`
}
/*
{"type": "hello", "pub_key":"I4GFRZlXOIPNoEe6rmlJy2LVAKQ/s/a89AH9B0EjaiQ="}

{"type":"auth", "sign":""}
*/
type ClientPushToken struct {
	Type string 	`json:"type"`
	Token string 	`json:"token"`
}

type ServerChallenge struct { 
	Type string		`json:"type"`
	Nonce []byte 	`json:"nonce"`
}

type ServerOk struct {
	Type string 	`json:"type"`
}

type ServerError struct {
	Type string 	`json:"type"`
	Message string 	`json:"message"`
}

type ServerDeliver struct {
	Type string 		`json:"type"`
	MessageId int64 	`json:"message_id"`
	From []byte 		`json:"from"`
	Blob []byte 		`json:"blob"`
}

type MessageType struct {
	Type string `json:"type"`
}

func Encode(msg any) ([]byte, error) {
	return json.Marshal(msg)
}

func Decode(data []byte) (any, error) {
	var msgType MessageType
	if err := json.Unmarshal(data, &msgType); err != nil {
		return nil, err
	}

	switch msgType.Type {
		case CLIENT_HELLO:
			var clHello ClientHello
			if err := json.Unmarshal(data, &clHello); err != nil {
				return nil, err
			}
			return clHello, nil
		case CLIENT_AUTH:
			var clAuth ClientAuth
			if err := json.Unmarshal(data, &clAuth); err != nil {
				return nil, err
			}
			return clAuth, nil
		case CLIENT_SEND:
			var clSend ClientSend
			if err := json.Unmarshal(data, &clSend); err != nil {
				return nil, err
			}
			return clSend, nil
		case CLIENT_ACK:
			var clAck ClientAck
			if err := json.Unmarshal(data, &clAck); err != nil {
				return nil, err
			}
			return clAck, nil
		case CLIENT_PUSH_TOKEN:
			var clPushToken ClientPushToken
			if err := json.Unmarshal(data, &clPushToken); err != nil {
				return nil, err
			}
			return clPushToken, nil
		default:
			return nil, fmt.Errorf("unknown type: %s", msgType.Type)
	}
}