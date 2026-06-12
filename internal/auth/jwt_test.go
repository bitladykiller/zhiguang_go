package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/zhiguang/app/pkg/config"
)

func TestJWTServiceIssueAndValidateTokenPair(t *testing.T) {
	cfg := writeJWTConfigFiles(t, false)

	service, err := NewJWTService(cfg)
	if err != nil {
		t.Fatalf("NewJWTService() error = %v", err)
	}

	user := &User{
		ID:       42,
		Nickname: "alice",
	}
	tokenPair, err := service.IssueTokenPair(user)
	if err != nil {
		t.Fatalf("IssueTokenPair() error = %v", err)
	}
	if tokenPair.RefreshTokenID == "" {
		t.Fatal("IssueTokenPair() should populate RefreshTokenID")
	}

	accessClaims, err := service.ValidateToken(tokenPair.AccessToken)
	if err != nil {
		t.Fatalf("ValidateToken(access) error = %v", err)
	}
	if accessClaims.UserID() != 42 {
		t.Fatalf("accessClaims.UserID() = %d, want 42", accessClaims.UserID())
	}
	if accessClaims.TokenType() != tokenTypeAccess {
		t.Fatalf("accessClaims.TokenType() = %q, want %q", accessClaims.TokenType(), tokenTypeAccess)
	}

	refreshClaims, err := service.ValidateToken(tokenPair.RefreshToken)
	if err != nil {
		t.Fatalf("ValidateToken(refresh) error = %v", err)
	}
	if refreshClaims.TokenType() != tokenTypeRefresh {
		t.Fatalf("refreshClaims.TokenType() = %q, want %q", refreshClaims.TokenType(), tokenTypeRefresh)
	}

	parsed, _, err := new(jwt.Parser).ParseUnverified(tokenPair.AccessToken, &JWTClaims{})
	if err != nil {
		t.Fatalf("ParseUnverified(access) error = %v", err)
	}
	if got := parsed.Header["kid"]; got != cfg.KeyID {
		t.Fatalf("access token kid = %v, want %q", got, cfg.KeyID)
	}
}

func TestJWTServiceValidateTokenRejectsUnexpectedSigningMethod(t *testing.T) {
	cfg := writeJWTConfigFiles(t, false)

	service, err := NewJWTService(cfg)
	if err != nil {
		t.Fatalf("NewJWTService() error = %v", err)
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"uid":        42,
		"token_type": tokenTypeAccess,
		"exp":        time.Now().Add(time.Minute).Unix(),
	})
	tokenString, err := token.SignedString([]byte("secret"))
	if err != nil {
		t.Fatalf("SignedString() error = %v", err)
	}

	if _, err := service.ValidateToken(tokenString); err == nil {
		t.Fatal("ValidateToken() should reject unexpected signing method")
	}
}

func TestLoadPrivateKeySupportsPKCS1Fallback(t *testing.T) {
	cfg := writeJWTConfigFiles(t, true)

	privateKey, err := loadPrivateKey(cfg.PrivateKeyPath)
	if err != nil {
		t.Fatalf("loadPrivateKey() error = %v", err)
	}
	if privateKey.N.BitLen() == 0 {
		t.Fatal("loadPrivateKey() returned invalid RSA key")
	}

	publicKey, err := loadPublicKey(cfg.PublicKeyPath)
	if err != nil {
		t.Fatalf("loadPublicKey() error = %v", err)
	}
	if publicKey.N.BitLen() == 0 {
		t.Fatal("loadPublicKey() returned invalid RSA key")
	}
}

func writeJWTConfigFiles(t *testing.T, pkcs1Private bool) *config.JWTConfig {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}

	privateBlockType := "PRIVATE KEY"
	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatalf("MarshalPKCS8PrivateKey() error = %v", err)
	}
	if pkcs1Private {
		privateBlockType = "RSA PRIVATE KEY"
		privateDER = x509.MarshalPKCS1PrivateKey(privateKey)
	}

	publicDER, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey() error = %v", err)
	}

	dir := t.TempDir()
	privatePath := filepath.Join(dir, "private.pem")
	publicPath := filepath.Join(dir, "public.pem")

	if err := os.WriteFile(privatePath, pem.EncodeToMemory(&pem.Block{Type: privateBlockType, Bytes: privateDER}), 0o600); err != nil {
		t.Fatalf("write private key: %v", err)
	}
	if err := os.WriteFile(publicPath, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER}), 0o600); err != nil {
		t.Fatalf("write public key: %v", err)
	}

	return &config.JWTConfig{
		Issuer:          "zhiguang-test",
		KeyID:           http.CanonicalHeaderKey("kid-test"),
		PrivateKeyPath:  privatePath,
		PublicKeyPath:   publicPath,
		AccessTokenTTL:  time.Minute,
		RefreshTokenTTL: 24 * time.Hour,
	}
}
