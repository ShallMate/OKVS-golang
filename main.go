package main

import (
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"time"

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

func TestXorAt() {
	ba1 := bitarray.MustParse("1010-1010 1010-1010 10")
	ba2 := bitarray.MustParse("1111-0000")
	fmt.Println(ba1)
	fmt.Println(ba2)
	fmt.Printf("% b\n", ba1.XorAt(0, ba2))
	fmt.Printf("% b\n", ba1.XorAt(1, ba2))
	fmt.Printf("% b\n", ba1.XorAt(10, ba2))
}

func TestLeft() {
	b1 := bitarray.MustParse("100111")
	//b2 := bitarray.MustParse("101000")
	b1t := b1.ShiftLeft(3)
	fmt.Println(b1t)
	fmt.Println(b1)
}

func main() {
	n := 200000
	e := 1.03
	m := int(math.Round(float64(n) * e))

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
		N: n,
		M: m,
		W: 360,
		P: make([]*bitarray.BitArray, m),
	}
	s1 := time.Now()
	okvs.Encode(kvs)
	end := time.Since(s1)
	fmt.Println(end)

	for i := 0; i < int(n); i++ {
		v := okvs.Decode(kvs[i].Key).Int64()

		if v != int64(kvs[i].Value) {
			fmt.Printf("decoding false")
		}

		//fmt.Println(v)
		//fmt.Println(kvs[i].Value)

	}
}

/*
func main() {
	n := 200000
	e := 1.03
	m := int(math.Round(float64(n) * e))

	// 创建长度为 n 的 KV 结构体切片
	kvs := make([]okvs.KVBF, n)

	// 输出 KV 结构体切片
	for i := 0; i < int(n); i++ {
		key := generateRandomBytes(8)              // 生成长度为8的随机字节切片作为key
		value := rand.Uint32()                     // 生成随机的uint32切片作为value
		kvs[i] = okvs.KVBF{Key: key, Value: value} // 将key和value赋值给KV结构体
	}
	//fmt.Printf("KV slice: %+v\n", kvs)
	okvs := okvs.OKVSBF{
		N: n,
		M: m,
		W: 360,
		P: make([]*bitarray.Buffer, m),
	}
	s1 := time.Now()
	okvs.Encode(kvs)
	end := time.Since(s1)
	fmt.Println(end)

	for i := 0; i < int(n); i++ {
		v := okvs.Decode(kvs[i].Key)

		if v != kvs[i].Value {
			fmt.Printf("decoding false")
		}

		//fmt.Println(v)
		//fmt.Println(kvs[i].Value)

	}
}
*/
