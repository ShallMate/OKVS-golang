package main

import (
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"sync"
	"time"

	okvs "github.com/OurOKVS/OKVS"
	"github.com/bits-and-blooms/bitset"
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

func TestXor() {
	b1 := bitarray.NewBuffer(360)
	b2 := bitarray.NewBuffer(360)
	s := time.Now()
	b1.XorAt(0, b2)
	e := time.Since(s)
	fmt.Println(e)
	fmt.Println(b1)
}

func TestXor1() {
	b1 := bitset.New(365)
	b1.SetAll()
	b2 := bitset.New(360)
	b2.SetAll()
	s := time.Now()
	b1.InPlaceSymmetricDifference(b2)
	e := time.Since(s)
	fmt.Println(e)
	fmt.Println(b2.String())
}

func TestXor2() {
	a, _ := new(big.Int).SetString("123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890", 10)
	bigB, _ := new(big.Int).SetString("987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210", 10)

	// 重置计时器
	s := time.Now()
	a.Xor(a, bigB)
	e := time.Since(s)
	fmt.Println(e)
}

func Pdecode(wg *sync.WaitGroup, i int, iend int, kvs []okvs.KV, okvs *okvs.OKVS, n int) {
	defer wg.Done()
	for k := i; k < iend; k++ {
		okvs.Decode(kvs[k%n].Key).Int64()
	}
}

/*
func main() {
	n := 1048576
	n1 := 1 << 24
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
	w := 256
	okvs := okvs.OKVS{
		N: n,
		M: m,
		W: w,
		R: m - w,
		P: make([]*bitarray.BitArray, m),
	}
	numCPU := runtime.NumCPU()
	fmt.Println("Number of CPUs:", numCPU)

	// 设置最大 CPU 核数
	runtime.GOMAXPROCS(numCPU)
	var wg sync.WaitGroup
	s1 := time.Now()
	okvs.Encode(kvs)
	end := time.Since(s1)
	fmt.Println("e =", e)
	fmt.Printf("encoing n = %d, time = %s\n", n1, end)
	threadnum := 128
	block := n / threadnum
	s2 := time.Now()
	for i := 0; i < threadnum; i++ {
		wg.Add(1)
		go Pdecode(&wg, i*block, (i+1)*block, kvs, &okvs, n)
	}
	wg.Wait()
	end = time.Since(s2)
	fmt.Printf("decoing n = %d, time = %s\n", n1, end)
	//TestXor()
	//TestXor2()
}
*/

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

/*
func main() {
	f, _ := os.OpenFile("cpu.profile", os.O_CREATE|os.O_RDWR, 0644)
	defer f.Close()
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	n := 10000
	e := 1.03
	m := int(math.Round(float64(n) * e))

	// 创建长度为 n 的 KV 结构体切片
	kvs := make([]okvs.KVB, n)

	// 输出 KV 结构体切片
	for i := 0; i < int(n); i++ {
		key := generateRandomBytes(8)             // 生成长度为8的随机字节切片作为key
		value, _ := rand1.Prime(rand1.Reader, 32) // 生成随机的uint32切片作为value
		kvs[i] = okvs.KVB{Key: key, Value: value} // 将key和value赋值给KV结构体
	}
	//fmt.Printf("KV slice: %+v\n", kvs)
	w := 360
	okvs := okvs.OKVSB{
		N: n,
		M: m,
		W: w,
		R: m - w,
		P: make([]*big.Int, m),
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
	//TestXor()
	//TestXor2()
}
*/

func main() {
	n := 65536
	//n1 := 1 << 24
	e := 1.01
	m := int(math.Round(float64(n) * e))

	// 创建长度为 n 的 KV 结构体切片
	kvs := make([]okvs.KVBK, n)
	// 输出 KV 结构体切片
	for i := 0; i < int(n); i++ {
		key := generateRandomBytes(8)              // 生成长度为8的随机字节切片作为key
		value := rand.Uint32()                     // 生成随机的uint32切片作为value
		kvs[i] = okvs.KVBK{Key: key, Value: value} // 将key和value赋值给KV结构体
	}
	//fmt.Printf("KV slice: %+v\n", kvs)
	w := 360
	okvs := okvs.OKVSBK{
		N: n,
		M: m,
		W: w,
		B: w / 8,
		R: m - w,
		P: make([]uint32, m),
	}
	s1 := time.Now()
	okvs.Encode(kvs)
	end := time.Since(s1)
	fmt.Println("e =", e)
	fmt.Printf("encoing n = %d, time = %s\n", n, end)
	//threadnum := 128
	//block := n / threadnum
	s2 := time.Now()
	for i := 1; i < n; i++ {
		//fmt.Println(okvs.Decode(kvs[i].Key))
		//fmt.Println(kvs[i].Value)
		//okvs.Decode(kvs[i].Key)
	}
	//wg.Wait()
	end = time.Since(s2)
	fmt.Printf("decoing n = %d, time = %s\n", n, end)
	//TestXor()
	//TestXor2()
}
