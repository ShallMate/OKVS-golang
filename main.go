package main

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime/pprof"
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

func TestLeft() {
	b1 := bitarray.MustParse("100111")
	//b2 := bitarray.MustParse("101000")
	b1t := b1.ShiftLeft(3)
	fmt.Println(b1t)
	fmt.Println(b1)
}

func main() {
	f, _ := os.OpenFile("cpu.profile", os.O_CREATE|os.O_RDWR, 0644)
	defer f.Close()
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	n := 10000
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
	w := 360
	okvs := okvs.OKVS{
		N: n,
		M: m,
		W: w,
		R: m - w,
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
	f, _ := os.OpenFile("cpu.profile", os.O_CREATE|os.O_RDWR, 0644)
	defer f.Close()
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
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
