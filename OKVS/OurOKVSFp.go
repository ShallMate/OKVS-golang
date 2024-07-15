package okvs

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"sort"
	"sync"
)

// 定义System结构体
type SystemFp struct {
	Pos   int
	Row   []*big.Int
	Value *big.Int
}

var one = big.NewInt(1)
var zero = big.NewInt(0)

type OKVSFp struct {
	N int //okvs存储的k-v长度
	M int //okvs的实际长度
	W int //随机块的长度
	P []*big.Int
	Q *big.Int
}

type KVFp struct {
	Key   *big.Int //key
	Value *big.Int //value
}

func NewOkvsFp(n, logq int, e float64) OKVSFp {
	m := int(math.Round(float64(n) * 1.03))
	// 创建长度为 n 的 KV 结构体切片
	q, _ := rand.Prime(rand.Reader, logq)
	okvs := OKVSFp{
		N: n,                   // 假设 N 的长度为 10
		M: m,                   // 假设 M 的长度为 10
		W: 360,                 // 假设 W 的长度为 32
		P: make([]*big.Int, m), // 初始化 P 切片长度为 10
		Q: q,
	}
	return okvs
}

func (r *OKVSFp) hash1(bytesize int, key *big.Int) int {
	hashRange := r.M - r.W
	hashkey := HashToFixedSize(bytesize, key.Bytes())
	hashkeyint := int(binary.BigEndian.Uint32(hashkey)) % hashRange
	return hashkeyint
}

func (r *OKVSFp) hash2(key []byte) []byte {
	bandsize := r.W / 8
	hashBytes := HashToFixedSize(bandsize, key)
	return hashBytes
}

func (r *OKVSFp) Init(kvs []KVFp) []SystemFp {
	systems := make([]SystemFp, r.N)
	for i := 0; i < r.N; i++ {
		systems[i].Pos = r.hash1(4, kvs[i].Key)
		systems[i].Row = make([]*big.Int, r.W)
		row := r.hash2(kvs[i].Key.Bytes())
		for j := 0; j < r.W; j++ {
			if getBit(row[j/8], j%8) {
				systems[i].Row[j] = one
			} else {
				systems[i].Row[j] = zero
			}
		}
		//fmt.Println(systems[i].Row)
		systems[i].Value = kvs[i].Value
	}
	for i := 0; i < r.M; i++ {
		r.P[i] = zero
	}
	//fmt.Println(r.P)
	return systems
}

func (r *OKVSFp) Encode(kvs []KVFp) *OKVSFp {
	n := r.N
	if len(kvs) != n {
		fmt.Println("r.N must equal to len(kvs)")
		return nil
	}
	systems := r.Init(kvs)
	//fmt.Println(systems)
	sort.Slice(systems, func(i, j int) bool {
		return systems[i].Pos < systems[j].Pos
	})
	//fmt.Println(systems)
	piv := make([]int, n)
	for i := range piv {
		piv[i] = -1
	}
	var bigPool = sync.Pool{
		New: func() interface{} {
			return new(big.Int)
		},
	}
	q := r.Q
	w := r.W
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		for j := 0; j < w; j++ {
			if systems[i].Row[j].Cmp(zero) != 0 {
				piv[i] = j + systems[i].Pos
				for k := i + 1; k < n; k++ {
					if systems[k].Pos > piv[i] {
						break
					}
					wg.Add(1)
					go func(k, i, j int) {
						defer wg.Done()
						posk := piv[i] - systems[k].Pos
						poskk := j - posk
						t := new(big.Int).ModInverse(systems[i].Row[j], q)
						if systems[k].Row[posk].Cmp(zero) != 0 {
							tt := new(big.Int).Mul(t, systems[k].Row[posk])
							tt = tt.Mod(tt, q)
							shiftnum := w - j + posk
							for s := posk; s < shiftnum; s++ {
								systems[k].Row[s] = new(big.Int).Sub(systems[k].Row[s], new(big.Int).Mul(tt, systems[i].Row[poskk+s]))
								systems[k].Row[s] = systems[k].Row[s].Mod(systems[k].Row[s], q)
							}
							systems[k].Value = new(big.Int).Sub(systems[k].Value, new(big.Int).Mul(tt, systems[i].Value))
							systems[k].Value = systems[k].Value.Mod(systems[k].Value, q)
						}
					}(k, i, j)
				}
				wg.Wait()
				break
			}
		}
		if piv[i] == -1 {
			fmt.Println("Fail to generate at {i}th row!", i)
			return nil
		}
	}
	t := new(big.Int)
	index := 0
	for i := n - 1; i >= 0; i-- {
		res := big.NewInt(0)
		for j := 0; j < w; j++ {
			temp := bigPool.Get().(*big.Int)
			if systems[i].Row[j].Cmp(zero) != 0 {
				index = j + systems[i].Pos
				res = res.Add(res, temp.Mul(systems[i].Row[j], r.P[index]))
			}
			bigPool.Put(temp)
		}
		res = res.Sub(systems[i].Value, res)
		t = t.ModInverse(systems[i].Row[piv[i]-systems[i].Pos], q)
		res = res.Mul(t, res)
		r.P[piv[i]] = new(big.Int).Mod(res, q)
	}
	return r
}

func (r *OKVSFp) Decode(key *big.Int) *big.Int {
	pos := r.hash1(4, key)
	row := r.hash2(key.Bytes())
	res := big.NewInt(0)
	index := 0
	for j := 0; j < r.W; j++ {
		if getBit(row[j/8], j%8) {
			//r.P[j] = r.P[j].ToWidth(32, bitarray.AlignRight)
			index = j + pos
			res = new(big.Int).Add(res, r.P[index])
		}
	}
	res = new(big.Int).Mod(res, r.Q)
	return res

}

func (r *OKVSFp) ParDecode(kvs []KVFp) []*big.Int {
	block := 4096
	i := 0
	end := i + block
	res := make([]*big.Int, r.N)
	var wg sync.WaitGroup
	for {
		if end >= r.N {
			end = r.N
		}
		if i >= r.N {
			break
		}
		wg.Add(1)
		go func(i, end int) {
			defer wg.Done()
			for j := i; j < end; j++ {
				res[j] = r.Decode(kvs[j].Key)
			}
		}(i, end)
		i = i + block
		end = end + block
	}
	wg.Wait()
	return res
}

func (r *OKVSFp) Scalar(k *big.Int) []*big.Int {
	block := 4096
	i := 0
	end := i + block
	res := make([]*big.Int, r.N)
	var wg sync.WaitGroup
	q := r.Q
	for {
		if end >= r.N {
			end = r.N
		}
		if i >= r.N {
			break
		}
		wg.Add(1)
		go func(i, end int) {
			defer wg.Done()
			for j := i; j < end; j++ {
				r.P[j] = r.P[j].Mul(r.P[j], k)
				r.P[j] = r.P[j].Mod(r.P[j], q)
			}
		}(i, end)
		i = i + block
		end = end + block
	}
	wg.Wait()
	return res
}
