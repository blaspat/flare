// Package sync — transparent encryption at rest for synced files.
//
// Files on disk in watch directories are encrypted using AES-256-GCM when an
// encryption key is configured. Each file is wrapped in the format:
//
//	[12-byte random nonce][AES-GCM ciphertext + 16-byte auth tag]
//
// The nonce is generated once per file write via crypto/rand and stored
// inline. Decryption reads the nonce, then authenticates and decrypts the
// rest. The file path and name are NOT encrypted, only the content.
//
// When no key is configured (or the key is empty), all operations are
// pass-through — files are read/written as plaintext. This means encryption
// is fully opt-in and backward compatible.
package sync

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
)

// AES-GSM nonce size (12 bytes is recommended for GCM).
const gcmNonceSize = 12

// CryptoManager handles transparent AES-256-GCM encryption and decryption
// of file content. When the key is nil or empty, all methods are pass-through
// (plaintext only), making encryption fully opt-in.
type CryptoManager struct {
	key     []byte // 32 bytes for AES-256; nil = disabled
	enabled bool
}

// NewCryptoManagerFromHex parses a hex-encoded 64-character AES-256 key and
// returns a CryptoManager. Pass an empty string to disable encryption.
func NewCryptoManagerFromHex(hexKey string) *CryptoManager {
	if hexKey == "" {
		return &CryptoManager{enabled: false}
	}
	key, err := hex.DecodeString(hexKey)
	if err != nil {
		slog.Warn("encryption key is not valid hex — disabling encryption", "err", err)
		return &CryptoManager{enabled: false}
	}
	if len(key) != 32 {
		slog.Warn("encryption key must be 32 bytes (64 hex chars) — disabling encryption",
			"got_bytes", len(key))
		return &CryptoManager{enabled: false}
	}
	return &CryptoManager{
		key:     key,
		enabled: true,
	}
}

// NewCryptoManagerFromBytes creates a CryptoManager from a raw 32-byte key.
// Pass nil to disable encryption.
func NewCryptoManagerFromBytes(key []byte) *CryptoManager {
	if len(key) != 32 {
		return &CryptoManager{enabled: false}
	}
	k := make([]byte, 32)
	copy(k, key)
	return &CryptoManager{
		key:     k,
		enabled: true,
	}
}

// Enabled returns true when a valid AES-256 key was configured.
func (cm *CryptoManager) Enabled() bool {
	return cm.enabled
}

// Encrypt takes plaintext bytes and returns nonce || ciphertext ready for
// storage. When encryption is disabled this is a no-op (returns data as-is).
func (cm *CryptoManager) Encrypt(plaintext []byte) ([]byte, error) {
	if !cm.enabled {
		return plaintext, nil
	}
	block, err := aes.NewCipher(cm.key)
	if err != nil {
		return nil, fmt.Errorf("aes: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("gcm: %w", err)
	}
	nonce := make([]byte, gcmNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("nonce: %w", err)
	}
	ciphertext := gcm.Seal(nil, nonce, plaintext, nil)
	out := make([]byte, gcmNonceSize+len(ciphertext))
	copy(out[:gcmNonceSize], nonce)
	copy(out[gcmNonceSize:], ciphertext)
	return out, nil
}

// Decrypt takes nonce || ciphertext and returns the original plaintext.
// When encryption is disabled this is a no-op (returns data as-is).
func (cm *CryptoManager) Decrypt(data []byte) ([]byte, error) {
	if !cm.enabled {
		return data, nil
	}
	if len(data) < gcmNonceSize+1 {
		return nil, errors.New("encrypted data too short")
	}
	block, err := aes.NewCipher(cm.key)
	if err != nil {
		return nil, fmt.Errorf("aes: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("gcm: %w", err)
	}
	nonce := data[:gcmNonceSize]
	ciphertext := data[gcmNonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("gcm auth: %w", err)
	}
	return plaintext, nil
}

// ReadEncrypted reads a file from disk and decrypts its content. When
// encryption is disabled this is equivalent to os.ReadFile.
func (cm *CryptoManager) ReadEncrypted(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if !cm.enabled {
		return data, nil
	}
	return cm.Decrypt(data)
}

// ReadDecryptedWithFallback reads a file from disk, tries to decrypt it,
// and falls back to raw content if the file isn't encrypted or can't be
// decrypted (e.g., encryption was just enabled and existing files are
// plaintext). A warning is logged on fallback.
func (cm *CryptoManager) ReadDecryptedWithFallback(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if !cm.enabled {
		return data, nil
	}
	if len(data) < gcmNonceSize+1 {
		return data, nil
	}
	plain, err := cm.Decrypt(data)
	if err != nil {
		slog.Warn("file decryption failed, reading as plaintext (encryption newly enabled?)",
			"path", path, "err", err)
		return data, nil
	}
	return plain, nil
}

// WriteEncrypted encrypts plaintext data and writes it to the given path.
// When encryption is disabled this is equivalent to os.WriteFile with 0644.
func (cm *CryptoManager) WriteEncrypted(path string, data []byte) error {
	if !cm.enabled {
		return os.WriteFile(path, data, 0644)
	}
	out, err := cm.Encrypt(data)
	if err != nil {
		return err
	}
	return os.WriteFile(path, out, 0644)
}
