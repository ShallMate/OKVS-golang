package okvs

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"sync"
	"unsafe"

	"golang.org/x/crypto/blake2b"
)

/*
#cgo CXXFLAGS: -O2 -Wall -march=native -mavx2 -finline-functions
#cgo LDFLAGS: -lstdc++
#include <stdint.h>

// 声明 C++ 函数
void xor_shift_simd(uint8_t* result, uint8_t* arr1, uint8_t* arr2, int shifts, int shiftnum);
extern uint32_t optimized_xor(uint8_t* row, uint32_t* r_P, int W, int Pos, int Value);

*/
import "C"

// 定义System结构体
type SystemBK struct {
	Pos   int
	BPos  int
	Row   []byte
	Value uint32
}

type OKVSBK struct {
	N int //okvs存储的k-v长度
	M int //okvs的实际长度
	W int //随机块的长度
	B int //桶的个度
	R int // hashrange
	P []uint32
}

// 序列化 OKVSBK 结构体到文件
func SerializeOKVSBK(filename string, data OKVSBK) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// 写入基本数据
	err = binary.Write(file, binary.LittleEndian, int32(data.N))
	if err != nil {
		return err
	}
	err = binary.Write(file, binary.LittleEndian, int32(data.M))
	if err != nil {
		return err
	}
	err = binary.Write(file, binary.LittleEndian, int32(data.W))
	if err != nil {
		return err
	}
	err = binary.Write(file, binary.LittleEndian, int32(data.B))
	if err != nil {
		return err
	}
	err = binary.Write(file, binary.LittleEndian, int32(data.R))
	if err != nil {
		return err
	}

	// 写入 P 数组长度和内容
	err = binary.Write(file, binary.LittleEndian, int32(len(data.P)))
	if err != nil {
		return err
	}
	err = binary.Write(file, binary.LittleEndian, data.P)
	if err != nil {
		return err
	}

	return nil
}

// 从文件中反序列化 OKVSBK 结构体
func DeserializeOKVSBK(filename string) (OKVSBK, error) {
	file, err := os.Open(filename)
	if err != nil {
		return OKVSBK{}, err
	}
	defer file.Close()

	var data OKVSBK

	// 读取基本数据
	var n, m, w, b, r int32
	err = binary.Read(file, binary.LittleEndian, &n)
	if err != nil {
		return OKVSBK{}, err
	}
	err = binary.Read(file, binary.LittleEndian, &m)
	if err != nil {
		return OKVSBK{}, err
	}
	err = binary.Read(file, binary.LittleEndian, &w)
	if err != nil {
		return OKVSBK{}, err
	}
	err = binary.Read(file, binary.LittleEndian, &b)
	if err != nil {
		return OKVSBK{}, err
	}
	err = binary.Read(file, binary.LittleEndian, &r)
	if err != nil {
		return OKVSBK{}, err
	}

	data.N = int(n)
	data.M = int(m)
	data.W = int(w)
	data.B = int(b)
	data.R = int(r)

	// 读取 P 数组
	var pLen int32
	err = binary.Read(file, binary.LittleEndian, &pLen)
	if err != nil {
		return OKVSBK{}, err
	}
	data.P = make([]uint32, pLen)
	err = binary.Read(file, binary.LittleEndian, data.P)
	if err != nil {
		return OKVSBK{}, err
	}

	return data, nil
}

func HashToFixedSize(bytesize int, key []byte) []byte {
	if bytesize <= 64 {
		hash, _ := blake2b.New(bytesize, nil)
		hash.Write(key)
		return hash.Sum(nil)
	}

	// If bytesize > 64, generate multiple hashes and concatenate them
	numHashes := (bytesize + 63) / 64 // Calculate the number of 64-byte chunks needed
	hashResult := make([]byte, 0, numHashes*64)
	for i := 0; i < numHashes; i++ {
		hash, _ := blake2b.New(64, nil)
		hash.Write(key)
		// Add different data to each hash to ensure uniqueness
		hash.Write([]byte(fmt.Sprintf("%d", i)))
		hashResult = append(hashResult, hash.Sum(nil)...)
	}
	return hashResult[:bytesize]
}

var bitMasks = [8]byte{
	0x80, // 1000 0000
	0x40, // 0100 0000
	0x20, // 0010 0000
	0x10, // 0001 0000
	0x08, // 0000 1000
	0x04, // 0000 0100
	0x02, // 0000 0010
	0x01, // 0000 0001
}

func getBit(b byte, n int) bool {
	return b&bitMasks[n] > 0
}

type KVBK struct {
	Key   []byte //key
	Value uint32 //value
}

func (r *OKVSBK) hash1(bytesize int, key []byte) int {
	hashkey := HashToFixedSize(bytesize, key)
	//fmt.Println(r.R)
	hashkeyint := int(binary.BigEndian.Uint32(hashkey)) % r.R
	return hashkeyint
}

func (r *OKVSBK) hash2(key []byte) []byte {
	bandsize := r.W / 8
	hashBytes := HashToFixedSize(bandsize, key)
	return hashBytes
}

func (r *OKVSBK) SetLine(i int, system *SystemBK, kv *KVBK) {
	system.Pos = r.hash1(4, kv.Key)
	system.BPos = int(system.Pos / 8)
	system.Pos = system.BPos * 8
	system.Row = r.hash2(kv.Key)
	system.Value = kv.Value
}

func (r *OKVSBK) Init(kvs []KVBK) []SystemBK {
	systems := make([]SystemBK, r.N)
	for i := 0; i < r.N; i++ {
		r.SetLine(i, &systems[i], &kvs[i])
	}
	return systems
}

func (r *OKVSBK) Encode(kvs []KVBK) *OKVSBK {
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
	//var wg sync.WaitGroup
	//block := 4096
	for i := 0; i < r.N; i++ {
		for j := 0; j < r.W; j++ {
			if getBit(systems[i].Row[int(j/8)], j%8) {
				piv[i] = j + systems[i].Pos
				for k := i + 1; k < r.N; k++ {
					if systems[k].Pos > piv[i] {
						break
					}
					posk := piv[i] - systems[k].Pos
					if getBit(systems[k].Row[int(posk/8)], posk%8) {
						shiftnum := systems[k].BPos - systems[i].BPos
						shifts := r.B - shiftnum
						//result := make([]byte, shifts)
						C.xor_shift_simd(
							(*C.uint8_t)(unsafe.Pointer(&systems[k].Row[0])),
							(*C.uint8_t)(unsafe.Pointer(&systems[i].Row[0])),
							(*C.uint8_t)(unsafe.Pointer(&systems[k].Row[0])),
							C.int(shifts),
							C.int(shiftnum),
						)
						systems[k].Value = systems[k].Value ^ systems[i].Value
					}

				}
				//
				break
			}
		}
		if piv[i] == -1 {
			fmt.Println("Fail to generate at {i}th row!", i)
			os.Exit(1)
			return nil
		}
	}

	for i := r.N - 1; i >= 0; i-- {
		res := uint32(0)
		pos := systems[i].Pos
		row := systems[i].Row
		for j := 0; j < r.W; j++ {
			if getBit(row[j/8], j%8) {
				index := pos + j
				res = res ^ r.P[index]
			}
		}
		r.P[piv[i]] = res ^ systems[i].Value
	}
	return r
}

func (r *OKVSBK) ShiftRowBK(wg *sync.WaitGroup, i int, iend int, pivi int, systems *[]SystemBK) {
	defer wg.Done()
	for k := i; k < iend; k++ {
		if (*systems)[k].Pos <= pivi {
			posk := pivi - (*systems)[k].Pos
			if getBit((*systems)[k].Row[int(posk/8)], posk%8) {
				shiftnum := (*systems)[k].BPos - (*systems)[i].BPos
				for b := 0; b < r.B-shiftnum; b++ {
					(*systems)[k].Row[b] = (*systems)[k].Row[b] ^ (*systems)[i].Row[b+shiftnum]
				}
				(*systems)[k].Value = (*systems)[k].Value ^ (*systems)[i].Value
			}
		}
	}
}

func (r *OKVSBK) Decode(key []byte) uint32 {
	pos := r.hash1(4, key)
	pos = int(pos/8) * 8
	row := r.hash2(key)
	var res uint32 = 0
	for j := pos; j < r.W+pos; j++ {
		if getBit(row[(j-pos)/8], (j-pos)%8) {
			res = res ^ r.P[j]
		}
	}
	return res

}

func (r *OKVSBK) DecodewithCheck(key []byte) (uint32, bool) {
	pos := r.hash1(4, key)
	pos = int(pos/8) * 8
	row := r.hash2(key)
	var res uint32 = 0
	for j := pos; j < r.W+pos; j++ {
		if getBit(row[(j-pos)/8], (j-pos)%8) {
			res = res ^ r.P[j]
		}
	}
	ok := true
	if res > uint32(r.N) {
		ok = false
	}
	return res, ok

}

func (r *OKVSBK) ParDecode(kvs []KVBK) []uint32 {
	block := 2048
	i := 0
	end := i + block
	res := make([]uint32, r.N)
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
				if res[j] != kvs[j].Value {
					fmt.Println("decoding error")
					os.Exit(1)
				}
			}
		}(i, end)
		i = i + block
		end = end + block
	}
	wg.Wait()
	return res
}
