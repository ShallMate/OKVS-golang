package okvs

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"sync"
	"unsafe"
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
type SystemECC struct {
	Pos   int
	BPos  int
	Row   []byte
	Value uint32
}

type OKVSECC struct {
	N int //okvs存储的k-v长度
	M int //okvs的实际长度
	W int //随机块的长度
	B int //桶的个度
	R int // hashrange
	P []uint32
}

// 序列化 OKVSBK 结构体到文件
func SerializeOKVSECC(filename string, data OKVSECC) error {
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
func DeserializeOKVSECC(filename string) (OKVSECC, error) {
	file, err := os.Open(filename)
	if err != nil {
		return OKVSECC{}, err
	}
	defer file.Close()

	var data OKVSECC

	// 读取基本数据
	var n, m, w, b, r int32
	err = binary.Read(file, binary.LittleEndian, &n)
	if err != nil {
		return OKVSECC{}, err
	}
	err = binary.Read(file, binary.LittleEndian, &m)
	if err != nil {
		return OKVSECC{}, err
	}
	err = binary.Read(file, binary.LittleEndian, &w)
	if err != nil {
		return OKVSECC{}, err
	}
	err = binary.Read(file, binary.LittleEndian, &b)
	if err != nil {
		return OKVSECC{}, err
	}
	err = binary.Read(file, binary.LittleEndian, &r)
	if err != nil {
		return OKVSECC{}, err
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
		return OKVSECC{}, err
	}
	data.P = make([]uint32, pLen)
	err = binary.Read(file, binary.LittleEndian, data.P)
	if err != nil {
		return OKVSECC{}, err
	}

	return data, nil
}

type KVECC struct {
	Key   []byte //key
	Value uint32 //value
}

func (r *OKVSECC) hash1(key []byte) int {

	hashkey := key[:4]
	//fmt.Println(r.R)
	hashkeyint := int(binary.BigEndian.Uint32(hashkey)) % r.R
	return hashkeyint
}

func (r *OKVSECC) hash2(key []byte) []byte {
	bandsize := r.W / 8
	hashBytes := key[:bandsize]
	return hashBytes
}

func (r *OKVSECC) SetLine(i int, system *SystemECC, kv *KVECC) {
	system.Pos = r.hash1(kv.Key)
	//fmt.Println(system.Pos)
	system.BPos = int(system.Pos / 8)
	system.Pos = system.BPos * 8
	system.Row = r.hash2(kv.Key)
	//fmt.Println(system.Row)
	system.Value = kv.Value
}

func (r *OKVSECC) Init(kvs []KVECC) []SystemECC {
	systems := make([]SystemECC, r.N)
	for i := 1; i < r.N; i++ {
		r.SetLine(i, &systems[i], &kvs[i])
	}
	//fmt.Println("system 5", systems[5].Row)
	//fmt.Println(systems[5].Row)
	return systems
}

func (r *OKVSECC) Encode(kvs []KVECC) *OKVSECC {
	if len(kvs) != r.N {
		fmt.Println("r.N must equal to len(kvs)")
		return nil
	}
	systems := r.Init(kvs)

	sort.Slice(systems[1:], func(i, j int) bool {
		return systems[i+1].Pos < systems[j+1].Pos
	})
	piv := make([]int, r.N)
	for i := range piv {
		piv[i] = -1
	}
	//var wg sync.WaitGroup
	//block := 4096
	//fmt.Println(systems[5].Row)
	for i := 1; i < r.N; i++ {
		for j := 0; j < r.W; j++ {
			if getBit(systems[i].Row[int(j/8)], j%8) {
				piv[i] = j + systems[i].Pos
				for k := i + 1; k < r.N; k++ {
					if systems[k].Pos > piv[i] {
						break
					}
					posk := piv[i] - systems[k].Pos
					//fmt.Println(k)
					//fmt.Println(systems[k].Row)
					//fmt.Println(systems[5].Row)
					//fmt.Println(int(posk / 8))
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
						/*
							for b := 0; b < shifts; b++ {
								systems[k].Row[b] = systems[k].Row[b] ^ systems[i].Row[b+shiftnum]
							}
						*/
						//copy(systems[k].Row[:shifts], result)
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

	for i := r.N - 1; i >= 1; i-- {
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

func (r *OKVSECC) ShiftRowBK(wg *sync.WaitGroup, i int, iend int, pivi int, systems *[]SystemECC) {
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

func (r *OKVSECC) Decode(key []byte) uint32 {
	if len(key) == 0 {
		return 0
	}
	pos := r.hash1(key)
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

func (r *OKVSECC) DecodewithCheck(key []byte) (uint32, bool) {
	ok := true
	if len(key) == 0 {
		return 0, ok
	}
	pos := r.hash1(key)
	pos = int(pos/8) * 8
	row := r.hash2(key)
	var res uint32 = 0
	for j := pos; j < r.W+pos; j++ {
		if getBit(row[(j-pos)/8], (j-pos)%8) {
			res = res ^ r.P[j]
		}
	}
	if res > uint32(r.N) {
		ok = false
	}
	return res, ok

}

func (r *OKVSECC) ParDecode(kvs []KVECC) []uint32 {
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
					fmt.Println(res[j])
					fmt.Println(kvs[j].Value)
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
