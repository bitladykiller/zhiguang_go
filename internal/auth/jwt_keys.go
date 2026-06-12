package auth

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
)

// loadPrivateKey 读取 PEM 编码的 RSA 私钥文件并解析为 Go 的 rsa.PrivateKey。
//
// 支持两种私钥格式（兼容性处理）：
//   - PKCS#8：较新的标准格式，由 openssl genpkey 生成（推荐）
//   - PKCS#1：较旧的 RSA-only 格式，由 openssl genrsa 生成
//
// 函数调用说明：
//   - os.ReadFile(path):
//     一次性读取整个 PEM 文件到 []byte。
//   - pem.Decode(data):
//     解码 PEM 块（找到 "-----BEGIN XXX-----" 和 "-----END XXX-----" 之间的内容）。
//     返回 *pem.Block 结构体，包含类型（如 "RSA PRIVATE KEY"）和 DER 编码的字节。
//   - x509.ParsePKCS8PrivateKey(block.Bytes):
//     x509 包提供的 PKCS#8 格式私钥解析函数。
//     PKCS#8 是一种通用的私钥格式，支持 RSA、ECDSA、Ed25519 等多种算法。
//     返回 interface{}，需要通过类型断言转为具体的 *rsa.PrivateKey。
//   - x509.ParsePKCS1PrivateKey(block.Bytes):
//     x509 包提供的 PKCS#1 格式 RSA 私钥解析函数。
//     这是 RSA 私钥的传统格式，只支持 RSA。
//     如果 PKCS#8 解析失败，会回退尝试此格式。
func loadPrivateKey(path string) (*rsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("no PEM block found")
	}

	// 先尝试解析 PKCS#8（较新的标准格式）
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err == nil {
		rsaKey, ok := key.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("key is not an RSA private key")
		}
		return rsaKey, nil
	}

	// 如果失败则回退到 PKCS#1（较旧格式）
	rsaKey, pkcs1Err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if pkcs1Err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", pkcs1Err)
	}
	return rsaKey, nil
}

// loadPublicKey 读取 PEM 编码的 RSA 公钥并解析为 Go 的 rsa.PublicKey。
//
// 函数调用说明：
//   - x509.ParsePKIXPublicKey(block.Bytes):
//     解析 SubjectPublicKeyInfo（PKIX）格式的公钥。
//     这是标准的 X.509 公钥格式，支持 RSA、ECDSA 等多种算法。
//     对应 openssl 的 `openssl rsa -pubin -in public.pem -RSAPublicKey_in` 格式。
//     注意与 x509.ParsePKCS1PublicKey() 的区别：前者是通用的 PKIX 格式，
//     后者是 RSA-only 的 PKCS#1 格式。
func loadPublicKey(path string) (*rsa.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("no PEM block found")
	}

	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key: %w", err)
	}

	rsaKey, ok := key.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("key is not an RSA public key")
	}

	return rsaKey, nil
}
