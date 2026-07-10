package sync

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testKeyHex is a valid 32-byte hex key for tests.
const testKeyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// TestNewCryptoManagerFromHex_ValidKey verifies that a valid hex key creates an
// enabled CryptoManager.
func TestNewCryptoManagerFromHex_ValidKey(t *testing.T) {
	cm := NewCryptoManagerFromHex(testKeyHex)
	if !cm.Enabled() {
		t.Fatal("expected enabled CryptoManager with valid key")
	}
}

// TestNewCryptoManagerFromHex_EmptyString verifies that an empty hex key
// creates a disabled (pass-through) CryptoManager.
func TestNewCryptoManagerFromHex_EmptyString(t *testing.T) {
	cm := NewCryptoManagerFromHex("")
	if cm.Enabled() {
		t.Fatal("expected disabled CryptoManager with empty key")
	}
}

// TestNewCryptoManagerFromHex_InvalidHex verifies that an invalid hex string
// creates a disabled CryptoManager with a warning logged.
func TestNewCryptoManagerFromHex_InvalidHex(t *testing.T) {
	cm := NewCryptoManagerFromHex("not-a-hex-string!")
	if cm.Enabled() {
		t.Fatal("expected disabled CryptoManager with invalid hex")
	}
}

// TestNewCryptoManagerFromHex_WrongLength verifies that a hex string that
// doesn't decode to exactly 32 bytes creates a disabled CryptoManager.
func TestNewCryptoManagerFromHex_WrongLength(t *testing.T) {
	// 31 bytes = 62 hex chars (wrong).
	shortKey := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd"
	cm := NewCryptoManagerFromHex(shortKey)
	if cm.Enabled() {
		t.Fatal("expected disabled CryptoManager with wrong-length key")
	}
}

// TestNewCryptoManagerFromBytes_ValidKey verifies that a valid 32-byte key
// creates an enabled CryptoManager.
func TestNewCryptoManagerFromBytes_ValidKey(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	cm := NewCryptoManagerFromBytes(key)
	if !cm.Enabled() {
		t.Fatal("expected enabled CryptoManager with valid 32-byte key")
	}
}

// TestNewCryptoManagerFromBytes_Nil verifies that nil creates a disabled
// CryptoManager.
func TestNewCryptoManagerFromBytes_Nil(t *testing.T) {
	cm := NewCryptoManagerFromBytes(nil)
	if cm.Enabled() {
		t.Fatal("expected disabled CryptoManager with nil key")
	}
}

// TestNewCryptoManagerFromBytes_ShortKey verifies that a short key creates
// a disabled CryptoManager.
func TestNewCryptoManagerFromBytes_ShortKey(t *testing.T) {
	cm := NewCryptoManagerFromBytes([]byte("too-short"))
	if cm.Enabled() {
		t.Fatal("expected disabled CryptoManager with short key")
	}
}

// TestEncryptDecrypt_RoundTrip verifies that encrypting and then decrypting
// returns the original plaintext.
func TestEncryptDecrypt_RoundTrip(t *testing.T) {
	cm := NewCryptoManagerFromHex(testKeyHex)
	plaintext := []byte("Hello, Flare! This is a test message.")

	ciphertext, err := cm.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// Ciphertext must be nonce (12) + ciphertext (≥ len(plaintext)).
	if len(ciphertext) < 12+len(plaintext) {
		t.Errorf("ciphertext too short: got %d bytes, want at least %d",
			len(ciphertext), 12+len(plaintext))
	}

	decrypted, err := cm.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Errorf("round-trip mismatch:\n  want: %x\n  got:  %x", plaintext, decrypted)
	}
}

// TestEncryptDecrypt_EmptyPlaintext verifies that encrypting/decrypting an
// empty byte slice works correctly.
func TestEncryptDecrypt_EmptyPlaintext(t *testing.T) {
	cm := NewCryptoManagerFromHex(testKeyHex)

	ciphertext, err := cm.Encrypt([]byte{})
	if err != nil {
		t.Fatalf("Encrypt empty failed: %v", err)
	}

	decrypted, err := cm.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt empty failed: %v", err)
	}

	if len(decrypted) != 0 {
		t.Errorf("expected empty result, got %d bytes", len(decrypted))
	}
}

// TestEncryptDecrypt_LargeContent verifies round-trip with 1 MB of random data.
func TestEncryptDecrypt_LargeContent(t *testing.T) {
	cm := NewCryptoManagerFromHex(testKeyHex)
	plaintext := make([]byte, 1024*1024) // 1 MB
	if _, err := rand.Read(plaintext); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	ciphertext, err := cm.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	decrypted, err := cm.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Error("large content round-trip failed")
	}
}

// TestDecrypt_TooShort verifies that decrypting data smaller than the nonce
// size returns an error.
func TestDecrypt_TooShort(t *testing.T) {
	cm := NewCryptoManagerFromHex(testKeyHex)

	_, err := cm.Decrypt([]byte("too-short"))
	if err == nil {
		t.Fatal("expected error for too-short ciphertext")
	}
}

// TestDecrypt_TamperedData verifies that modifying the ciphertext causes an
// authentication error.
func TestDecrypt_TamperedData(t *testing.T) {
	cm := NewCryptoManagerFromHex(testKeyHex)
	plaintext := []byte("This data must not be tampered with.")

	ciphertext, err := cm.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// Tamper with the first byte after the nonce.
	tampered := make([]byte, len(ciphertext))
	copy(tampered, ciphertext)
	tampered[12] ^= 0xFF // flip bits in ciphertext

	_, err = cm.Decrypt(tampered)
	if err == nil {
		t.Fatal("expected GCM auth error for tampered ciphertext")
	}
	if !strings.Contains(err.Error(), "gcm auth") && !strings.Contains(err.Error(), "authentication") {
		t.Fatalf("expected auth-related error, got: %v", err)
	}
}

// TestDecrypt_WrongKey verifies that decrypting with a different key fails.
func TestDecrypt_WrongKey(t *testing.T) {
	cm1 := NewCryptoManagerFromHex(testKeyHex)
	// Use a different key for decryption.
	otherKey := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	cm2 := NewCryptoManagerFromHex(otherKey)

	plaintext := []byte("secret data")
	ciphertext, err := cm1.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	_, err = cm2.Decrypt(ciphertext)
	if err == nil {
		t.Fatal("expected GCM auth error for wrong key")
	}
}

// TestNonceUniqueness verifies that each Encrypt call produces a unique nonce
// (different ciphertext even for same plaintext).
func TestNonceUniqueness(t *testing.T) {
	cm := NewCryptoManagerFromHex(testKeyHex)
	plaintext := []byte("same content each time")

	c1, err := cm.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("first Encrypt: %v", err)
	}

	c2, err := cm.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("second Encrypt: %v", err)
	}

	if hex.EncodeToString(c1) == hex.EncodeToString(c2) {
		t.Fatal("expected different ciphertext (nonce should be unique)")
	}
}

// TestPassThrough_DisabledByEmptyKey verifies that when encryption is disabled
// (empty key), Encrypt and Decrypt are pass-through.
func TestPassThrough_DisabledByEmptyKey(t *testing.T) {
	cm := NewCryptoManagerFromHex("")
	plaintext := []byte("plaintext data")

	out, err := cm.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt (disabled) failed: %v", err)
	}
	if !bytes.Equal(out, plaintext) {
		t.Error("Encrypt pass-through should return input unchanged")
	}

	out2, err := cm.Decrypt(out)
	if err != nil {
		t.Fatalf("Decrypt (disabled) failed: %v", err)
	}
	if !bytes.Equal(out2, plaintext) {
		t.Error("Decrypt pass-through should return input unchanged")
	}
}

// TestPassThrough_DisabledByNilKey verifies pass-through with nil byte key.
func TestPassThrough_DisabledByNilKey(t *testing.T) {
	cm := NewCryptoManagerFromBytes(nil)
	plaintext := []byte("plaintext data")

	out, err := cm.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt (disabled) failed: %v", err)
	}
	if !bytes.Equal(out, plaintext) {
		t.Error("Encrypt pass-through should return input unchanged")
	}

	out2, err := cm.Decrypt(out)
	if err != nil {
		t.Fatalf("Decrypt (disabled) failed: %v", err)
	}
	if !bytes.Equal(out2, plaintext) {
		t.Error("Decrypt pass-through should return input unchanged")
	}
}

// TestReadEncrypted writes plaintext to disk, encrypts it in place, then
// verifies that ReadEncrypted returns the correct plaintext.
func TestReadEncrypted(t *testing.T) {
	cm := NewCryptoManagerFromHex(testKeyHex)
	dir := t.TempDir()
	path := filepath.Join(dir, "secret.txt")
	plaintext := []byte("encrypted at rest content")

	// Write encrypted data to disk.
	if err := cm.WriteEncrypted(path, plaintext); err != nil {
		t.Fatalf("WriteEncrypted: %v", err)
	}

	// Read raw file to verify it's NOT plaintext.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if bytes.Equal(raw, plaintext) {
		t.Error("file content should not be plaintext when encryption enabled")
	}

	// Read via ReadEncrypted to verify round-trip.
	got, err := cm.ReadEncrypted(path)
	if err != nil {
		t.Fatalf("ReadEncrypted: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Error("ReadEncrypted returned wrong plaintext")
	}
}

// TestWriteEncrypted_Disabled verifies that WriteEncrypted writes plaintext
// when encryption is disabled.
func TestWriteEncrypted_Disabled(t *testing.T) {
	cm := NewCryptoManagerFromHex("") // disabled
	dir := t.TempDir()
	path := filepath.Join(dir, "plain.txt")
	data := []byte("plaintext data")

	if err := cm.WriteEncrypted(path, data); err != nil {
		t.Fatalf("WriteEncrypted (disabled): %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("WriteEncrypted (disabled) should write plaintext")
	}
}

// TestReadEncrypted_Disabled verifies that ReadEncrypted reads plaintext
// when encryption is disabled.
func TestReadEncrypted_Disabled(t *testing.T) {
	cm := NewCryptoManagerFromHex("") // disabled
	dir := t.TempDir()
	path := filepath.Join(dir, "plain.txt")
	data := []byte("plaintext data")

	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	got, err := cm.ReadEncrypted(path)
	if err != nil {
		t.Fatalf("ReadEncrypted (disabled): %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("ReadEncrypted (disabled) should return plaintext")
	}
}

// TestReadDecryptedWithFallback_LegacyFile verifies that a plaintext file
// (not encrypted) can be read when encryption is newly enabled, via fallback.
func TestReadDecryptedWithFallback_LegacyFile(t *testing.T) {
	cm := NewCryptoManagerFromHex(testKeyHex)
	dir := t.TempDir()
	path := filepath.Join(dir, "legacy.txt")
	plaintext := []byte("this file was created before encryption was enabled")

	// Write plaintext to disk (legacy file).
	if err := os.WriteFile(path, plaintext, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// ReadDecryptedWithFallback should succeed via fallback.
	got, err := cm.ReadDecryptedWithFallback(path)
	if err != nil {
		t.Fatalf("ReadDecryptedWithFallback: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Error("ReadDecryptedWithFallback should return original plaintext")
	}
}

// TestReadDecryptedWithFallback_ShortFile verifies that files shorter than the
// nonce size are treated as plaintext (not encrypted) by the fallback path.
func TestReadDecryptedWithFallback_ShortFile(t *testing.T) {
	cm := NewCryptoManagerFromHex(testKeyHex)
	dir := t.TempDir()
	path := filepath.Join(dir, "short.txt")
	plaintext := []byte("short")

	if err := os.WriteFile(path, plaintext, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	got, err := cm.ReadDecryptedWithFallback(path)
	if err != nil {
		t.Fatalf("ReadDecryptedWithFallback: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Error("ReadDecryptedWithFallback should return short file as-is")
	}
}

// TestReadDecryptedWithFallback_Disabled verifies that when encryption is
// disabled, ReadDecryptedWithFallback returns raw content without attempting
// decryption.
func TestReadDecryptedWithFallback_Disabled(t *testing.T) {
	cm := NewCryptoManagerFromHex("") // disabled
	dir := t.TempDir()
	path := filepath.Join(dir, "raw.txt")
	data := []byte("some raw content")

	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	got, err := cm.ReadDecryptedWithFallback(path)
	if err != nil {
		t.Fatalf("ReadDecryptedWithFallback: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("ReadDecryptedWithFallback (disabled) should return raw content")
	}
}

// TestReadDecryptedWithFallback_EncryptedFile verifies that an encrypted file
// is properly decrypted by ReadDecryptedWithFallback.
func TestReadDecryptedWithFallback_EncryptedFile(t *testing.T) {
	cm := NewCryptoManagerFromHex(testKeyHex)
	dir := t.TempDir()
	path := filepath.Join(dir, "encrypted.txt")
	plaintext := []byte("this is properly encrypted")

	// Write encrypted file.
	enc, err := cm.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if err := os.WriteFile(path, enc, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// ReadDecryptedWithFallback should decrypt correctly.
	got, err := cm.ReadDecryptedWithFallback(path)
	if err != nil {
		t.Fatalf("ReadDecryptedWithFallback: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Error("ReadDecryptedWithFallback should decrypt encrypted file")
	}
}

// TestMultipleEncryptsSameKey verifies that multiple encrypt/decrypt
// operations all work correctly with the same CryptoManager.
func TestMultipleEncryptsSameKey(t *testing.T) {
	cm := NewCryptoManagerFromHex(testKeyHex)
	inputs := []string{
		"message one",
		"another message",
		"third message with different length here",
		"",
		"x",
	}

	for _, input := range inputs {
		plaintext := []byte(input)
		cipher, err := cm.Encrypt(plaintext)
		if err != nil {
			t.Fatalf("Encrypt(%q): %v", input, err)
		}
		decrypted, err := cm.Decrypt(cipher)
		if err != nil {
			t.Fatalf("Decrypt(%q): %v", input, err)
		}
		if !bytes.Equal(plaintext, decrypted) {
			t.Errorf("round-trip failed for %q", input)
		}
	}
}

// TestCryptoManager_KeyIsolation verifies that the CryptoManager copies
// the key bytes passed to NewCryptoManagerFromBytes, preventing external
// modification.
func TestCryptoManager_KeyIsolation(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = 0xAB
	}

	cm := NewCryptoManagerFromBytes(key)
	// Modify the original key slice.
	for i := range key {
		key[i] = 0x00
	}

	if !cm.Enabled() {
		t.Fatal("CryptoManager should still be enabled after original key modified")
	}

	plaintext := []byte("test isolation")
	cipher, err := cm.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	decrypted, err := cm.Decrypt(cipher)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(plaintext, decrypted) {
		t.Error("round-trip failed after key slice mutation")
	}
}
