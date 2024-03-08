package main

import (
	"fmt"
	"math"
	"math/big"
	"math/rand"

	okvs "github.com/RBOKVS/OKVS"
	"github.com/tunabay/go-bitarray"
)

func generateRandomBytes(length int) []byte {
	bytes := make([]byte, length)
	for i := 0; i < length; i++ {
		bytes[i] = byte(rand.Intn(256)) // 生成一个0到255之间的随机数作为字节值
	}
	return bytes
}

func Testcoding() {
	for {
		value := rand.Uint32()
		Value := bitarray.NewFromInt(big.NewInt(int64(value)))
		Value = Value.ToWidth(32, bitarray.AlignRight)
		value1 := Value.ToInt().Int64()
		if value1 != int64(value) {
			fmt.Println("coding fail")
		}
		fmt.Println(value)
		fmt.Println(value1)
	}

}

func main() {
	n := 1000 // 假设长度为 5
	m := int(math.Round(float64(n) * 1.03))

	// 创建长度为 n 的 KV 结构体切片
	kvs := make([]okvs.KV, n)

	// 输出 KV 结构体切片
	for i := 0; i < int(n); i++ {
		key := generateRandomBytes(8)            // 生成长度为8的随机字节切片作为key
		value := rand.Uint32()                   // 生成随机的uint32切片作为value
		kvs[i] = okvs.KV{Key: key, Value: value} // 将key和value赋值给KV结构体
	}
	//fmt.Printf("KV slice: %+v\n", kvs)
	okvs := okvs.OKVS{
		N: n,                             // 假设 N 的长度为 10
		M: m,                             // 假设 M 的长度为 10
		W: 360,                           // 假设 W 的长度为 32
		P: make([]*bitarray.BitArray, m), // 初始化 P 切片长度为 10
	}
	okvs.Encode(kvs)
	for i := 0; i < int(n); i++ {
		v := okvs.Decode(kvs[i].Key).Int64()
		if v != int64(kvs[i].Value) {
			fmt.Printf("decoding false")
		}
	}
}
