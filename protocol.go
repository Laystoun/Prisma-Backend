package main

import (
	"encoding/json"
	"fmt"
)

const (
	CLIENT_HELLO = "hello"
	CLIENT_AUTH = "auth"
	CLIENT_SEND = "send"
	CLIENT_ACK = "ack"
	CLIENT_PUSH_TOKEN = "push_token"
	CLIENT_STATUS_REQUEST = "client_status_request"
	CLIENT_EVENT_ACK = "client_event_ack"
	CLIENT_DELETE_MESSAGE = "client_deleted_message"
)

const (
	SERVER_CHALLENGE = "challenge"
	SERVER_OK = "ok"
	SERVER_ERROR = "error"
	SERVER_DELIVER = "deliver"
	SERVER_STATUS_RESPONSE = "server_status_response"
	SERVER_EVENT = "event"
)

type ClientHello struct { 
	Type string 	`json:"type"`
	PubKey []byte 	`json:"pub_key"`
}

type ClientAuth struct {
	Type string 	`json:"type"`
	Sign []byte		`json:"sign"`
}

type ClientSend struct {
	Type string 			`json:"type"`
	To []byte 				`json:"to"`
	Blob []byte 			`json:"blob"`
	SendAt int64 			`json:"send_at"`
	ClientMessageId string 	`json:"client_message_id"`
}

type ClientAck struct {
	Type string 	`json:"type"`
	MessageId int64 `json:"message_id"`
}

type ClientPushToken struct {
	Type string 	`json:"type"`
	Token string 	`json:"token"`
}

type ClientStatusRequest struct {
	Type string 	`json:"type"`
	PubKey []byte 	`json:"pub_key"`
}

type ClientDeleteMessage struct {
    Type    string          `json:"type"`
    To      []byte          `json:"to"`
    Payload json.RawMessage `json:"payload"`
}

type ClientEventAck struct {
    Type      string `json:"type"`
    RequestID string `json:"request_id"`
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
	Type string 			`json:"type"`
	MessageId int64 		`json:"message_id"`
	From []byte 			`json:"from"`
	Blob []byte 			`json:"blob"`
	SendAt int64 			`json:"send_at"`
	ClientMessageId string 	`json:"client_message_id"`
}

type ServerStatusResponce struct {
	Type string 	`json:"type"`
	PubKey []byte 	`json:"pub_key"`
	Status bool 	`json:"online"`
}

type ServerEvent struct {
    Type      string          `json:"type"`
    RequestID string          `json:"request_id"`
    From      []byte          `json:"from"`
    EventType string          `json:"event_type"`
    Payload   json.RawMessage `json:"payload"`
}


type DeleteMessagePayload struct {
    ClientMessageIDs []string `json:"client_message_ids"`
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
		case CLIENT_STATUS_REQUEST:
			var clStatusRequest ClientStatusRequest
			if err := json.Unmarshal(data, &clStatusRequest); err != nil {
				return nil, err
			}
			return clStatusRequest, nil
		case CLIENT_DELETE_MESSAGE:
			var clDeleteMessage ClientDeleteMessage
			if err := json.Unmarshal(data, &clDeleteMessage); err != nil {
				return nil, err
			}
			return clDeleteMessage, nil
		case CLIENT_EVENT_ACK:
			var clEventAck ClientEventAck
			if err := json.Unmarshal(data, &clEventAck); err != nil {
				return nil, err
			}
			return clEventAck, nil
		default:
			return nil, fmt.Errorf("unknown type: %s", msgType.Type)
	}
}