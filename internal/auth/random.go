package auth

import (
	"crypto/rand"
	"math/big"
)

// randomInt 生成 [0, max) 范围内的密码学安全随机整数。
func randomInt(max int64) (int64, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(max))
	if err != nil {
		return 0, err
	}
	return n.Int64(), nil
}
