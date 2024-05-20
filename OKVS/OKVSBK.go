package okvs

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
)

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

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU()) // 使用所有可用的CPU核心
}

func getBit(b byte, n int) bool {
	n = 7 - n
	return (b & (1 << n)) > 0
}

type KVBK struct {
	Key   []byte //key
	Value uint32 //value
}

func (r *OKVSBK) hash1(bytesize int, key []byte) int {
	hashkey := HashToFixedSize(bytesize, key)
	hashkeyint := int(binary.BigEndian.Uint32(hashkey)) % r.R
	return hashkeyint
}

func (r *OKVSBK) hash2(pos int, key []byte) []byte {
	hashBytes := HashToFixedSize(r.B, key)
	return hashBytes
}

func (r *OKVSBK) SetLine(i int, system *SystemBK, kv *KVBK) {
	system.Pos = r.hash1(4, kv.Key)
	system.BPos = int(system.Pos / 8)
	system.Pos = system.BPos * 8
	system.Row = r.hash2(system.Pos, kv.Key)
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
	//fmt.Println("开始编码")
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
	block := r.N / 4096
	fmt.Println(block)
	fmt.Println(r.N)
	var wg sync.WaitGroup
	for i := 0; i < r.N; i++ {
		for j := 0; j < r.W; j++ {
			if getBit(systems[i].Row[int(j/8)], j%8) {
				piv[i] = j + systems[i].Pos
				q := i + 1
				for {
					if q+block < r.N {
						wg.Add(1)
						go func(i, q int) {
							defer wg.Done()
							for k := q; k < q+block; k++ {
								pivi := piv[i]
								if systems[k].Pos <= pivi {
									posk := piv[i] - systems[k].Pos
									if getBit(systems[k].Row[int(posk/8)], posk%8) {
										shiftnum := systems[k].BPos - systems[i].BPos
										for b := 0; b < r.B-shiftnum; b++ {
											if systems[i].Row[b+shiftnum] != 0 {
												systems[k].Row[b] = systems[k].Row[b] ^ systems[i].Row[b+shiftnum]
											}
										}
										systems[k].Value = systems[k].Value ^ systems[i].Value
									}
								}
							}
						}(i, q)
					} else {
						wg.Add(1)
						go func(i, q int) {
							defer wg.Done()
							for k := q; k < r.N; k++ {
								if systems[k].Pos <= piv[i] {
									posk := piv[i] - systems[k].Pos
									if getBit(systems[k].Row[int(posk/8)], posk%8) {
										shiftnum := systems[k].BPos - systems[i].BPos
										for b := 0; b < r.B-shiftnum; b++ {
											if systems[i].Row[b+shiftnum] != 0 {
												systems[k].Row[b] = systems[k].Row[b] ^ systems[i].Row[b+shiftnum]
											}
										}
										systems[k].Value = systems[k].Value ^ systems[i].Value
									}
								}
							}
						}(i, q)
						break
					}
					q = q + block
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
	for i := r.N - 1; i >= 0; i-- {
		var res uint32 = 0
		for j := 0; j < r.W; j++ {
			if getBit(systems[i].Row[int(j/8)], j%8) {
				index := systems[i].Pos + j
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

func (r *OKVSBK) ParEncode(kvs []KVBK) *OKVSBK {
	//fmt.Println("开始编码")
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
	var wg sync.WaitGroup
	for i := 0; i < r.N; i++ {
		for j := 0; j < r.W; j++ {
			if getBit(systems[i].Row[int(j/8)], j%8) {
				piv[i] = j + systems[i].Pos
				threadnum := 256
				for k := i + 1; ; k = k + threadnum {
					if k+threadnum < r.N {
						wg.Add(1)
						go r.ShiftRowBK(&wg, k, k+threadnum, piv[i], &systems)
					} else {
						wg.Add(1)
						go r.ShiftRowBK(&wg, k, r.N, piv[i], &systems)
						break
					}
				}
				break
			}
		}
		wg.Wait()
		if piv[i] == -1 {
			//fmt.Printf("Fail to generate at {%d}th row!\n", i)
			return nil
		}
	}
	for i := r.N - 1; i >= 0; i-- {
		var res uint32 = 0
		for j := 0; j < r.W; j++ {
			if getBit(systems[i].Row[int(j/8)], j%8) {
				index := systems[i].Pos + j
				res = res ^ r.P[index]
			}
		}
		r.P[piv[i]] = res ^ systems[i].Value
	}
	return r
}

func (r *OKVSBK) Decode(key []byte) uint32 {
	pos := r.hash1(4, key)
	pos = int(pos/8) * 8
	row := r.hash2(pos, key)
	var res uint32 = 0
	for j := pos; j < r.W+pos; j++ {
		if getBit(row[(j-pos)/8], (j-pos)%8) {
			res = res ^ r.P[j]
		}
	}
	return res

}

// 序列化 OKVSBK 结构体并保存到文件
func (r *OKVSBK) SerializeOKVSBKToFile(filename string) error {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(r)
	if err != nil {
		return err
	}

	// 创建并写入文件
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(buffer.Bytes())
	if err != nil {
		return err
	}
	return nil
}

// 从文件中读取并反序列化 OKVSBK 结构体
func DeserializeOKVSBKFromFile(filename string) (OKVSBK, error) {
	var okvsbk OKVSBK

	// 打开文件并读取数据
	file, err := os.Open(filename)
	if err != nil {
		return OKVSBK{}, err
	}
	defer file.Close()

	var buffer bytes.Buffer
	_, err = buffer.ReadFrom(file)
	if err != nil {
		return OKVSBK{}, err
	}

	decoder := gob.NewDecoder(&buffer)
	err = decoder.Decode(&okvsbk)
	if err != nil {
		return OKVSBK{}, err
	}

	return okvsbk, nil
}
