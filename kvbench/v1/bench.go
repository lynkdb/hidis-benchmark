// Copyright 2020 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kvbench

import (
	"strings"
)

type ResultStatus int

const (
	ResultOK  ResultStatus = 1
	ResultERR ResultStatus = 2
)

const (
	BenchTypeRandWrite uint64 = 1 << 0
	BenchTypeRandRead  uint64 = 1 << 1
	BenchTypeSeqWrite  uint64 = 1 << 2
	BenchTypeSeqRead   uint64 = 1 << 3

	BenchTypeNameRandWrite = "rand-write"
	BenchTypeNameRandRead  = "rand-read"
	BenchTypeNameSeqWrite  = "seq-write"
	BenchTypeNameSeqRead   = "seq-read"
)

var (
	benchTypeMap = map[string]uint64{
		BenchTypeNameRandWrite: BenchTypeRandWrite,
		BenchTypeNameRandRead:  BenchTypeRandRead,
		BenchTypeNameSeqWrite:  BenchTypeSeqWrite,
		BenchTypeNameSeqRead:   BenchTypeSeqRead,
	}
	benchTypeNameMap = map[uint64]string{
		BenchTypeRandWrite: BenchTypeNameRandWrite,
		BenchTypeRandRead:  BenchTypeNameRandRead,
		BenchTypeSeqWrite:  BenchTypeNameSeqWrite,
		BenchTypeSeqRead:   BenchTypeNameSeqRead,
	}
)

func uint64Allow(base, diff uint64) bool {
	return (diff & base) == diff
}

func benchTypeName(t uint64) string {
	if s, ok := benchTypeNameMap[t]; ok {
		return s
	}
	return ""
}

func benchType(name string) uint64 {
	if v, ok := benchTypeMap[name]; ok {
		return v
	}
	return 0
}

func benchTypes(s string) []uint64 {
	var (
		ar = strings.Split(s, ",")
		ta = uint64(0)
		ts = []uint64{}
	)
	for _, v := range ar {
		v2 := benchType(v)
		if v2 > 0 && !uint64Allow(ta, v2) {
			ts = append(ts, v2)
			ta = ta | v2
		}
	}
	return ts
}
