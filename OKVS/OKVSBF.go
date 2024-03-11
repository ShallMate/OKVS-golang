package okvs

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/tunabay/go-bitarray"
)

type SystemBF struct {
	Pos   int
	Row   *bitarray.Buffer
	Value *bitarray.Buffer
}

type OKVSBF struct {
	N int //okvs存储的k-v长度
	M int //okvs的实际长度
	W int //随机块的长度
	P []*bitarray.Buffer
}

type KVBF struct {
	Key   []byte //key
	Value uint32 //value
}

func (r *OKVSBF) hash1(bytesize int, key []byte) int {
	hashRange := r.M - r.W
	hashkey := HashToFixedSize(bytesize, key)
	hashkeyint := int(binary.BigEndian.Uint32(hashkey)) % hashRange
	return hashkeyint
}

func (r *OKVSBF) hash2(pos int, key []byte) *bitarray.Buffer {
	bandsize := int(r.W) / 8
	hashBytes := HashToFixedSize(bandsize, key)
	band := bitarray.NewBufferFromByteSlice(hashBytes)
	return band
}

func (r *OKVSBF) Init(kvs []KVBF) []SystemBF {
	systems := make([]SystemBF, r.N)
	for i := 0; i < r.N; i++ {
		systems[i].Pos = r.hash1(4, kvs[i].Key)
		systems[i].Row = r.hash2(systems[i].Pos, kvs[i].Key)
		systems[i].Value = bitarray.NewBuffer(32)
		systems[i].Value.PutUint32(kvs[i].Value)
	}
	return systems
}

func (r *OKVSBF) Encode(kvs []KVBF) *OKVSBF {
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
		for j := 0; j < r.W; j++ {
			if systems[i].Row.BitAt(j) == 1 {
				piv[i] = j + systems[i].Pos
				for k := i + 1; k < r.N; k++ {
					if systems[k].Pos <= piv[i] && systems[k].Row.BitAt(piv[i]-systems[k].Pos) == 1 {
						rowi := systems[i].Row.Slice(systems[k].Pos-systems[i].Pos, r.W)
						systems[k].Row.XorAt(0, rowi)
						systems[k].Value.XorAt(0, systems[i].Value)
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
		piv := piv[i]
		r.P[piv] = bitarray.NewBuffer(32)
		for j := 0; j < r.W; j++ {
			if systems[i].Row.BitAt(j) == 1 {
				index := systems[i].Pos + j
				if r.P[index].IsZero() {
					r.P[index] = bitarray.NewBuffer(32)
				}
				r.P[piv].XorAt(0, r.P[index])
			}
		}
		r.P[piv].XorAt(0, systems[i].Value)
	}
	return r
}

func (r *OKVSBF) Decode(key []byte) uint32 {
	pos := r.hash1(4, key)
	row := r.hash2(pos, key)
	res := bitarray.NewBuffer(32)
	for j := pos; j < r.W+pos; j++ {
		if row.BitAt(j-pos) == 1 {
			res.XorAt(0, r.P[j])
		}
	}
	return res.Uint32()

}
