package config

import "fmt"

func validatePort(name string, port int) error {
	if port <= 0 || port > 65535 {
		return fmt.Errorf("%s must be in [1,65535]", name)
	}
	return nil
}

// itoa 在不引入 strconv 的前提下把 int 转成字符串。
//
// 功能：
//
//	将整数 n 通过除 10 取余的方式逐位分解，然后拼接为字符串。
//	支持负数和零。
//
// 参数：
//   - n: 待转换的整数
//
// 返回值：
//   - string: 整数的十进制字符串表示
//
// WHY 不使用 strconv.Itoa：
//
//	官方说明是在启动路径上减少一个标准库依赖能略微缩短编译时间。
//	该函数仅在 DSN() 和 Addr() 中被调用，性能不敏感，
//	因此自实现的开销可以忽略。
//
// 边界情况：
//   - n == 0 → 返回 "0"
//   - n < 0 → 返回 "-" + 绝对值的字符串（如 -42 → "-42"）
//   - n == math.MinInt → 取绝对值会溢出，但该函数仅在端口号上使用，
//     端口号始终为正数，因此不会有负值极端情况。
//
// 实现说明：
//
//	使用 [20]byte 固定长度数组作为缓冲区（最大 int64 十进制 19 位 + 负号），
//	从尾部往前填充，最后切片转换为字符串。这比多次字符串拼接更高效。
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
