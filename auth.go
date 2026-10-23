package main

import (
	"crypto/ed25519"
	"crypto/rand"
)

func NewChallenge() ([]byte, error) {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func VerifySignature(pubKey, nonce, sig []byte) bool {
	if len(pubKey) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize{
		return false
	}
	return ed25519.Verify(pubKey, nonce, sig)
}