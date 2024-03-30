package okvs

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"runtime"
	"sort"
	"sync"
)

// 定义System结构体
type SystemB struct {
	Pos   int
	Row   *big.Int
	Value *big.Int
}

type OKVSB struct {
	N int //okvs存储的k-v长度
	M int //okvs的实际长度
	W int //随机块的长度
	R int // hashrange
	P []*big.Int
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU()) // 使用所有可用的CPU核心
}

type KVB struct {
	Key   []byte   //key
	Value *big.Int //value
}

func (r *OKVSB) hash1(bytesize int, key []byte) int {
	hashkey := HashToFixedSize(bytesize, key)
	hashkeyint := int(binary.BigEndian.Uint32(hashkey)) % r.R
	return hashkeyint
}

func (r *OKVSB) hash2(pos int, key []byte) *big.Int {
	bandsize := r.W / 8
	hashBytes := HashToFixedSize(bandsize, key)
	band := new(big.Int).SetBytes(hashBytes)
	band = band.SetBit(band, r.W-1, 1)
	return band
}

func (r *OKVSB) SetLine(wg *sync.WaitGroup, i int, system *SystemB, kv *KVB) {
	defer wg.Done()
	system.Pos = r.hash1(4, kv.Key)
	system.Row = r.hash2(system.Pos, kv.Key)
	if system.Row.BitLen() != r.W {
		system.Row = system.Row.SetBit(system.Row, r.W-1, 0)
		fmt.Println(system.Row.BitLen())
	}
	system.Value = kv.Value
}

func (r *OKVSB) ShiftRow(wg *sync.WaitGroup, pivi int, systemk *SystemB, systemi *SystemB) {
	//defer wg.Done()
	if systemk.Pos <= pivi && systemk.Row.Bit(pivi-systemk.Pos) == 1 {
		rowi := systemi.Row.Lsh(systemi.Row, uint(systemk.Pos-systemi.Pos))
		/*
			if rowi.BitLen() != systemk.Row.BitLen() {
				fmt.Println("rowi len", rowi.BitLen())
			}
		*/
		systemk.Row = systemk.Row.Xor(systemk.Row, rowi)
		systemk.Value = systemk.Value.Xor(systemk.Value, systemi.Value)
	}
}

func (r *OKVSB) Init(kvs []KVB) []SystemB {
	var wg sync.WaitGroup
	systems := make([]SystemB, r.N)
	for i := 0; i < r.N; i++ {
		wg.Add(1)
		go r.SetLine(&wg, i, &systems[i], &kvs[i])
	}
	wg.Wait()
	return systems
}

func (r *OKVSB) Encode(kvs []KVB) *OKVSB {
	fmt.Println("开始编码")
	if len(kvs) != r.N {
		fmt.Println("r.N must equal to len(kvs)")
		return nil
	}
	systems := r.Init(kvs)
	//fmt.Println("初始化完毕")
	sort.Slice(systems, func(i, j int) bool {
		return systems[i].Pos < systems[j].Pos
	})
	//fmt.Println("排序完成")
	//fmt.Println("排序完毕")
	piv := make([]int, r.N)
	for i := range piv {
		piv[i] = -1
	}
	var wg sync.WaitGroup
	for i := 0; i < r.N; i++ {
		//fmt.Println(i)
		for j := 0; j < r.W; j++ {
			if systems[i].Row.Bit(j) == 1 {
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
		res := big.NewInt(0)
		//res = res.ToWidth(32, bitarray.AlignRight)
		for j := 0; j < r.W; j++ {
			if systems[i].Row.Bit(j) == 1 {
				index := systems[i].Pos + j
				if r.P[index] == nil {
					r.P[index] = big.NewInt(0)
				}
				res = res.Xor(res, r.P[index])
			}
		}
		r.P[piv[i]] = res.Xor(res, systems[i].Value)
	}
	return r
}

func (r *OKVSB) Decode(key []byte) *big.Int {
	pos := r.hash1(4, key)
	row := r.hash2(pos, key)
	res := big.NewInt(0)
	//res = res.ToWidth(32, bitarray.AlignRight)
	for j := pos; j < r.W+pos; j++ {
		if row.Bit(j-pos) == 1 {
			//index := j + pos
			if r.P[j] == nil {
				r.P[j] = big.NewInt(0)
			}
			res = res.Xor(res, r.P[j])
		}
	}
	return res

}
