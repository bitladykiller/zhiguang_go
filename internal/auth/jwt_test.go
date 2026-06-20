package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/zhiguang/app/pkg/config"
)

func generateTestRSAKeyPair(t *testing.T) (*rsa.PrivateKey, *rsa.PublicKey) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return privateKey, &privateKey.PublicKey
}

func writePEMPrivateKey(t *testing.T, path string, key *rsa.PrivateKey) {
	t.Helper()
	block := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create private key file: %v", err)
	}
	defer f.Close()
	if err := pem.Encode(f, block); err != nil {
		t.Fatalf("encode private key: %v", err)
	}
}

func writePEMPublicKey(t *testing.T, path string, key *rsa.PublicKey) {
	t.Helper()
	pubBytes, err := x509.MarshalPKIXPublicKey(key)
	if err != nil {
		t.Fatalf("marshal public key: %v", err)
	}
	block := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubBytes,
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create public key file: %v", err)
	}
	defer f.Close()
	if err := pem.Encode(f, block); err != nil {
		t.Fatalf("encode public key: %v", err)
	}
}

func writePEMPrivateKeyPKCS8(t *testing.T, path string, key *rsa.PrivateKey) {
	t.Helper()
	pkcs8Bytes, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal pkcs8: %v", err)
	}
	block := &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: pkcs8Bytes,
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create pkcs8 key file: %v", err)
	}
	defer f.Close()
	if err := pem.Encode(f, block); err != nil {
		t.Fatalf("encode pkcs8 key: %v", err)
	}
}

func TestLoadPrivateKey(t *testing.T) {
	t.Run("PKCS1 format", func(t *testing.T) {
		dir := t.TempDir()
		privKey, _ := generateTestRSAKeyPair(t)
		path := filepath.Join(dir, "private.pem")
		writePEMPrivateKey(t, path, privKey)

		loaded, err := loadPrivateKey(path)
		if err != nil {
			t.Fatalf("load private key: %v", err)
		}
		if loaded == nil {
			t.Fatal("loaded key is nil")
		}
	})

	t.Run("PKCS8 format", func(t *testing.T) {
		dir := t.TempDir()
		privKey, _ := generateTestRSAKeyPair(t)
		path := filepath.Join(dir, "private_pkcs8.pem")
		writePEMPrivateKeyPKCS8(t, path, privKey)

		loaded, err := loadPrivateKey(path)
		if err != nil {
			t.Fatalf("load private key (pkcs8): %v", err)
		}
		if loaded == nil {
			t.Fatal("loaded key is nil")
		}
	})

	t.Run("file not found", func(t *testing.T) {
		_, err := loadPrivateKey("/nonexistent/path/private.pem")
		if err == nil {
			t.Fatal("expected error for non-existent file")
		}
	})

	t.Run("invalid PEM format", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "invalid.pem")
		if err := os.WriteFile(path, []byte("not a pem"), 0644); err != nil {
			t.Fatalf("write file: %v", err)
		}
		_, err := loadPrivateKey(path)
		if err == nil {
			t.Fatal("expected error for invalid pem")
		}
	})

	t.Run("not an RSA key", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "bad.pem")
		block := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: []byte("not a valid der")}
		f, _ := os.Create(path)
		pem.Encode(f, block)
		f.Close()

		_, err := loadPrivateKey(path)
		if err == nil {
			t.Fatal("expected error for invalid key data")
		}
	})
}

func TestLoadPublicKey(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		dir := t.TempDir()
		_, pubKey := generateTestRSAKeyPair(t)
		path := filepath.Join(dir, "public.pem")
		writePEMPublicKey(t, path, pubKey)

		loaded, err := loadPublicKey(path)
		if err != nil {
			t.Fatalf("load public key: %v", err)
		}
		if loaded == nil {
			t.Fatal("loaded key is nil")
		}
	})

	t.Run("file not found", func(t *testing.T) {
		_, err := loadPublicKey("/nonexistent/path/public.pem")
		if err == nil {
			t.Fatal("expected error for non-existent file")
		}
	})

	t.Run("invalid PEM", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "invalid.pem")
		if err := os.WriteFile(path, []byte("bad data"), 0644); err != nil {
			t.Fatalf("write file: %v", err)
		}
		_, err := loadPublicKey(path)
		if err == nil {
			t.Fatal("expected error for invalid pem")
		}
	})
}

func fakeJwtConfig(privPath, pubPath string) *config.JwtConfig {
	return &config.JwtConfig{
		Issuer:          "test",
		KeyID:           "test-key",
		PrivateKeyPath:  privPath,
		PublicKeyPath:   pubPath,
		AccessTokenTTL:  time.Hour,
		RefreshTokenTTL: 24 * time.Hour,
	}
}

func newJwtServiceWithTempKeys(t *testing.T) (*JwtService, *rsa.PrivateKey) {
	t.Helper()
	dir := t.TempDir()
	privKey, pubKey := generateTestRSAKeyPair(t)
	privPath := filepath.Join(dir, "private.pem")
	pubPath := filepath.Join(dir, "public.pem")
	writePEMPrivateKey(t, privPath, privKey)
	writePEMPublicKey(t, pubPath, pubKey)

	svc, err := NewJwtService(fakeJwtConfig(privPath, pubPath))
	if err != nil {
		t.Fatalf("new jwt service: %v", err)
	}
	return svc, privKey
}

func TestValidateToken(t *testing.T) {
	t.Run("valid token", func(t *testing.T) {
		svc, _ := newJwtServiceWithTempKeys(t)
		user := &User{ID: 1, Nickname: "test"}
		pair, err := svc.IssueTokenPair(user)
		if err != nil {
			t.Fatalf("issue token pair: %v", err)
		}
		claims, err := svc.ValidateToken(pair.AccessToken)
		if err != nil {
			t.Fatalf("validate token: %v", err)
		}
		if claims.UserID() != 1 {
			t.Fatalf("expected userID 1, got %d", claims.UserID())
		}
	})

	t.Run("invalid signature", func(t *testing.T) {
		svc1, _ := newJwtServiceWithTempKeys(t)
		svc2, _ := newJwtServiceWithTempKeys(t)
		user := &User{ID: 1, Nickname: "test"}
		pair, _ := svc1.IssueTokenPair(user)
		_, err := svc2.ValidateToken(pair.AccessToken)
		if err == nil {
			t.Fatal("expected error for wrong public key")
		}
	})

	t.Run("malformed token string", func(t *testing.T) {
		svc, _ := newJwtServiceWithTempKeys(t)
		_, err := svc.ValidateToken("not.a.jwt")
		if err == nil {
			t.Fatal("expected error for malformed token")
		}
	})

	t.Run("expired token", func(t *testing.T) {
		dir := t.TempDir()
		privKey, pubKey := generateTestRSAKeyPair(t)
		privPath := filepath.Join(dir, "private.pem")
		pubPath := filepath.Join(dir, "public.pem")
		writePEMPrivateKey(t, privPath, privKey)
		writePEMPublicKey(t, pubPath, pubKey)

		cfg := fakeJwtConfig(privPath, pubPath)
		cfg.AccessTokenTTL = -time.Hour
		svc, err := NewJwtService(cfg)
		if err != nil {
			t.Fatalf("new jwt service: %v", err)
		}
		pair, err := svc.IssueTokenPair(&User{ID: 1})
		if err != nil {
			t.Fatalf("issue token: %v", err)
		}
		_, err = svc.ValidateToken(pair.AccessToken)
		if err == nil {
			t.Fatal("expected error for expired token")
		}
	})

	t.Run("algorithm confusion attack (HS256)", func(t *testing.T) {
		svc, privKey := newJwtServiceWithTempKeys(t)
		maliciousToken := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"sub": "1",
			"exp": time.Now().Add(time.Hour).Unix(),
		})
		maliciousToken.Header["kid"] = "test-key"
		hs256Key := []byte(fmt.Sprintf("%d", privKey.D))
		tokenStr, err := maliciousToken.SignedString(hs256Key)
		if err != nil {
			t.Fatalf("sign malicious token: %v", err)
		}
		_, err = svc.ValidateToken(tokenStr)
		if err == nil {
			t.Fatal("expected error for HS256 algorithm confusion")
		}
	})
}

func TestIssueTokenPair(t *testing.T) {
	svc, _ := newJwtServiceWithTempKeys(t)
	user := &User{ID: 42, Nickname: "testuser"}
	pair, err := svc.IssueTokenPair(user)
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	if pair.AccessToken == "" {
		t.Fatal("access token is empty")
	}
	if pair.RefreshToken == "" {
		t.Fatal("refresh token is empty")
	}
	if pair.RefreshTokenID == "" {
		t.Fatal("refresh token ID is empty")
	}
	if pair.AccessTokenExpiresAt.IsZero() {
		t.Fatal("access token expires at is zero")
	}
	if pair.RefreshTokenExpiresAt.IsZero() {
		t.Fatal("refresh token expires at is zero")
	}

	accessClaims, err := svc.ValidateToken(pair.AccessToken)
	if err != nil {
		t.Fatalf("validate access token: %v", err)
	}
	if accessClaims.UserID() != 42 {
		t.Fatalf("expected userID 42, got %d", accessClaims.UserID())
	}

	refreshClaims, err := svc.ValidateToken(pair.RefreshToken)
	if err != nil {
		t.Fatalf("validate refresh token: %v", err)
	}
	if refreshClaims.UserID() != 42 {
		t.Fatalf("expected userID 42 in refresh, got %d", refreshClaims.UserID())
	}
}

func TestJwtService_NewJwtService(t *testing.T) {
	t.Run("missing private key", func(t *testing.T) {
		dir := t.TempDir()
		_, pubKey := generateTestRSAKeyPair(t)
		pubPath := filepath.Join(dir, "public.pem")
		writePEMPublicKey(t, pubPath, pubKey)

		cfg := fakeJwtConfig("/nonexistent/private.pem", pubPath)
		_, err := NewJwtService(cfg)
		if err == nil {
			t.Fatal("expected error for missing private key")
		}
	})

	t.Run("missing public key", func(t *testing.T) {
		dir := t.TempDir()
		privKey, _ := generateTestRSAKeyPair(t)
		privPath := filepath.Join(dir, "private.pem")
		writePEMPrivateKey(t, privPath, privKey)

		cfg := fakeJwtConfig(privPath, "/nonexistent/public.pem")
		_, err := NewJwtService(cfg)
		if err == nil {
			t.Fatal("expected error for missing public key")
		}
	})
}

func TestValidateToken_RefreshTokenType(t *testing.T) {
	svc, _ := newJwtServiceWithTempKeys(t)
	pair, err := svc.IssueTokenPair(&User{ID: 1, Nickname: "t"})
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	claims, err := svc.ValidateToken(pair.RefreshToken)
	if err != nil {
		t.Fatalf("validate refresh: %v", err)
	}
	if claims.TokenType() != "refresh" {
		t.Fatalf("expected 'refresh', got '%s'", claims.TokenType())
	}
}
