package okvs

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"sort"

	"github.com/tunabay/go-bitarray"
	"golang.org/x/crypto/blake2b"
)

// 定义System结构体
type System struct {
	Pos   int
	Row   *bitarray.BitArray
	Value *bitarray.BitArray
}

/*
docstring for OKVS
:n: rows
:m: columns
:w: length of band
The choice of parameters:
m = (1 + epsilon)n
w = O(lambda / epsilon + log n)
For example:
m = 2^10, epsilon = 0.1,
==> n = (1+0.1) * 2^10
==> w = (lambda + 19.830) / 0.2751
*/
type OKVS struct {
	N int //okvs存储的k-v长度
	M int //okvs的实际长度
	W int //随机块的长度
	P []*bitarray.BitArray
}

type KV struct {
	Key   []byte //key
	Value uint32 //value
}

func HashToFixedSize(bytesize int, key []byte) []byte {
	//fmt.Println(bytesize)
	hash, _ := blake2b.New(bytesize, []byte(key))
	return hash.Sum(nil)[:]
}

func (r *OKVS) hash1(bytesize int, key []byte) int {
	hashRange := r.M - r.W
	hashkey := HashToFixedSize(bytesize, key)
	hashkeyint := int(binary.BigEndian.Uint32(hashkey)) % hashRange
	return hashkeyint
}

func (r *OKVS) hash2(pos int, key []byte) *bitarray.BitArray {
	bandsize := int(r.W) / 8
	hashBytes := HashToFixedSize(bandsize, key)
	band := bitarray.NewFromBytes(hashBytes, 0, r.W)
	//fmt.Println(pos)
	//fmt.Println("band1=", band)
	band = band.ToWidth(r.W+pos, bitarray.AlignRight)
	//fmt.Println("band2=", band)
	band = band.ToWidth(r.M, bitarray.AlignLeft)
	//fmt.Println("band3=", band)
	return band
}

func (r *OKVS) Init(kvs []KV) []System {
	systems := make([]System, r.N)
	for i := 0; i < r.N; i++ {
		systems[i].Pos = r.hash1(4, kvs[i].Key)
		systems[i].Row = r.hash2(systems[i].Pos, kvs[i].Key)
		systems[i].Value = bitarray.NewFromInt(big.NewInt(int64(kvs[i].Value)))
		systems[i].Value = systems[i].Value.ToWidth(32, bitarray.AlignRight)
	}
	return systems
}

func (r *OKVS) Encode(kvs []KV) *OKVS {
	if len(kvs) != r.N {
		fmt.Println("r.N must equal to len(kvs)")
		return nil
	}
	systems := r.Init(kvs)
	sort.Slice(systems, func(i, j int) bool {
		return systems[i].Pos < systems[j].Pos
	})
	piv := make([]int, r.N)
	for i := range piv {
		piv[i] = -1
	}
	for i := 0; i < r.N; i++ {
		for j := systems[i].Pos; j < r.W+systems[i].Pos; j++ {
			if systems[i].Row.BitAt(j) == 1 {
				piv[i] = j
				for k := i + 1; k < r.N; k++ {
					if systems[k].Pos <= piv[i] && systems[k].Row.BitAt(piv[i]) == 1 {
						systems[k].Row = systems[k].Row.Xor(systems[i].Row)
						systems[k].Value = systems[k].Value.Xor(systems[i].Value)
					}
				}
				break
			}
		}
		if piv[i] == -1 {
			fmt.Println("Fail to generate at {i}th row!", i)
			return nil
		}
	}
	for i := r.N - 1; i >= 0; i-- {
		//reszeroBytes := make([]byte, 4)
		res := bitarray.New(0)
		res = res.ToWidth(32, bitarray.AlignRight)
		for j := 0; j < r.M; j++ {
			if systems[i].Row.BitAt(j) == 1 {
				r.P[j] = r.P[j].ToWidth(32, bitarray.AlignRight)
				//fmt.Println(r.P[j])
				//fmt.Println(res)
				res = res.Xor(r.P[j])
			}
		}
		r.P[piv[i]] = res.Xor(systems[i].Value)
	}
	return r
}

func (r *OKVS) Decode(key []byte) *big.Int {
	pos := r.hash1(4, key)
	row := r.hash2(pos, key)
	res := bitarray.New(0)
	res = res.ToWidth(32, bitarray.AlignRight)
	for j := 0; j < r.M; j++ {
		if row.BitAt(j) == 1 {
			//r.P[j] = r.P[j].ToWidth(32, bitarray.AlignRight)
			res = res.Xor(r.P[j])
		}
	}
	return res.ToInt()

}
