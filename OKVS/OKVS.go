package okvs

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"runtime"
	"sort"
	"sync"

	"github.com/tunabay/go-bitarray"
	"golang.org/x/crypto/blake2b"
)

// 定义System结构体
type System struct {
	Pos   int
	Row   *bitarray.BitArray
	Value *bitarray.BitArray
}

type OKVS struct {
	N int //okvs存储的k-v长度
	M int //okvs的实际长度
	W int //随机块的长度
	R int // hashrange
	P []*bitarray.BitArray
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU()) // 使用所有可用的CPU核心
}

type KV struct {
	Key   []byte //key
	Value uint32 //value
}

func HashToFixedSize(bytesize int, key []byte) []byte {
	hash, _ := blake2b.New(bytesize, []byte(key))
	return hash.Sum(nil)[:]
}

func (r *OKVS) hash1(bytesize int, key []byte) int {
	hashkey := HashToFixedSize(bytesize, key)
	hashkeyint := int(binary.BigEndian.Uint32(hashkey)) % r.R
	return hashkeyint
}

func (r *OKVS) hash2(pos int, key []byte) *bitarray.BitArray {
	bandsize := r.W / 8
	hashBytes := HashToFixedSize(bandsize, key)
	band := bitarray.NewFromBytes(hashBytes, 0, r.W)
	//band = band.ToWidth(r.W+pos, bitarray.AlignRight)
	//band = band.ToWidth(r.M, bitarray.AlignLeft)
	return band
}

func (r *OKVS) SetLine(wg *sync.WaitGroup, i int, system *System, kv *KV) {
	defer wg.Done()
	system.Pos = r.hash1(4, kv.Key)
	system.Row = r.hash2(system.Pos, kv.Key)
	system.Value = bitarray.NewFromInt(big.NewInt(int64(kv.Value)))
	system.Value = system.Value.ToWidth(32, bitarray.AlignRight)
}

func (r *OKVS) ShiftRow(wg *sync.WaitGroup, pivi int, systemk *System, systemi *System) {
	//defer wg.Done()
	if systemk.Pos <= pivi && systemk.Row.BitAt(pivi-systemk.Pos) == 1 {
		rowi := systemi.Row.ShiftLeft(systemk.Pos - systemi.Pos)
		systemk.Row = systemk.Row.Xor(rowi)
		systemk.Value = systemk.Value.Xor(systemi.Value)
	}
}

func (r *OKVS) Init(kvs []KV) []System {
	var wg sync.WaitGroup
	systems := make([]System, r.N)
	for i := 0; i < r.N; i++ {
		wg.Add(1)
		go r.SetLine(&wg, i, &systems[i], &kvs[i])
	}
	wg.Wait()
	return systems
}

func (r *OKVS) Encode(kvs []KV) *OKVS {
	if len(kvs) != r.N {
		fmt.Println("r.N must equal to len(kvs)")
		return nil
	}
	systems := r.Init(kvs)
	//fmt.Println("初始化完毕")
	sort.Slice(systems, func(i, j int) bool {
		return systems[i].Pos < systems[j].Pos
	})
	//fmt.Println("排序完毕")
	piv := make([]int, r.N)
	for i := range piv {
		piv[i] = -1
	}
	var wg sync.WaitGroup
	for i := 0; i < r.N; i++ {
		for j := 0; j < r.W; j++ {
			if systems[i].Row.BitAt(j) == 1 {
				piv[i] = j + systems[i].Pos
				for k := i + 1; k < r.N; k++ {
					//wg.Add(1)
					r.ShiftRow(&wg, piv[i], &systems[k], &systems[i])
				}
				//wg.Wait()
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
		for j := 0; j < r.W; j++ {
			if systems[i].Row.BitAt(j) == 1 {
				index := systems[i].Pos + j
				r.P[index] = r.P[index].ToWidth(32, bitarray.AlignRight)
				res = res.Xor(r.P[index])
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
	for j := pos; j < r.W+pos; j++ {
		if row.BitAt(j-pos) == 1 {
			//index := j + pos
			res = res.Xor(r.P[j])
		}
	}
	return res.ToInt()

}
