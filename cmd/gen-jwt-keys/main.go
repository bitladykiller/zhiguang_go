package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	privatePath := flag.String("private", "config/keys/private.pem", "path to the RSA private key")
	publicPath := flag.String("public", "config/keys/public.pem", "path to the RSA public key")
	bits := flag.Int("bits", 2048, "RSA key size in bits")
	flag.Parse()

	if *bits < 2048 {
		exitf("refusing to generate an RSA key smaller than 2048 bits")
	}

	privateKey, err := rsa.GenerateKey(rand.Reader, *bits)
	if err != nil {
		exitf("generate RSA key: %v", err)
	}

	if err := writePrivateKey(*privatePath, privateKey); err != nil {
		exitf("write private key: %v", err)
	}
	if err := writePublicKey(*publicPath, &privateKey.PublicKey); err != nil {
		exitf("write public key: %v", err)
	}
}

func writePrivateKey(path string, key *rsa.PrivateKey) error {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}

	block := &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: der,
	}

	return writePEM(path, block, 0o600)
}

func writePublicKey(path string, key *rsa.PublicKey) error {
	der, err := x509.MarshalPKIXPublicKey(key)
	if err != nil {
		return err
	}

	block := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: der,
	}

	return writePEM(path, block, 0o644)
}

func writePEM(path string, block *pem.Block, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	data := pem.EncodeToMemory(block)
	if data == nil {
		return fmt.Errorf("encode PEM block")
	}

	return os.WriteFile(path, data, perm)
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
