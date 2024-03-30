package okvs

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sort"
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
	bandsize := r.W / 8
	hashBytes := HashToFixedSize(bandsize, key)
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
	for i := 0; i < r.N; i++ {
		//fmt.Println(i)
		for j := 0; j < r.W; j++ {
			//fmt.Println(systems[i].Row)
			if getBit(systems[i].Row[int(j/8)], j%8) {
				piv[i] = j + systems[i].Pos
				for k := i + 1; k < r.N; k++ {
					if systems[k].Pos <= piv[i] {
						posk := piv[i] - systems[k].Pos
						if getBit(systems[k].Row[int(posk/8)], posk%8) {
							shiftnum := systems[k].BPos - systems[i].BPos
							for b := 0; b < r.B-shiftnum; b++ {
								systems[k].Row[b] = systems[k].Row[b] ^ systems[i].Row[b+shiftnum]
							}
							systems[k].Value = systems[k].Value ^ systems[i].Value
						}
					}
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
		var res uint32 = 0
		//res = res.ToWidth(32, bitarray.AlignRight)
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
	//res = res.ToWidth(32, bitarray.AlignRight)
	for j := pos; j < r.W+pos; j++ {
		if getBit(row[(j-pos)/8], (j-pos)%8) {
			//index := j + pos
			res = res ^ r.P[j]
		}
	}
	return res

}
