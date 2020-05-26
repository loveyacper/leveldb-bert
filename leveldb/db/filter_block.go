package leveldb

// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

import (
	"bytes"
)

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
type FilterBlockBuilder struct {
	policy FilterPolicy
	keys   []byte // Flattened key contents
	starts []int  // Starting index in keys of each key

	filter        []byte // Flattened filter result from CreateFilter
	filterOffsets []int
}

const (
	// Generate new filter every 2KB of data
	kFilterBaseLg uint64 = 11
	kFilterBase   uint64 = 1 << kFilterBaseLg
)

func (fbb *FilterBlockBuilder) StartBlock(blockOffset uint64) {
	filterIndex := int(blockOffset / kFilterBase)
	debug.Printf("StartBlock %v, index %v", blockOffset, filterIndex)
	if filterIndex < len(fbb.filterOffsets) {
		debug.Fatalf("Wrong filterIndex %v < len(fbb.filterOffsets) %v", filterIndex, len(fbb.filterOffsets))
	}

	for filterIndex > len(fbb.filterOffsets) {
		fbb.generateFilter()
	}
}

func (fbb *FilterBlockBuilder) AddKey(key []byte) {
	fbb.starts = append(fbb.starts, len(fbb.keys))
	fbb.keys = append(fbb.keys, key...)
}

func (fbb *FilterBlockBuilder) Finish() []byte {
	if len(fbb.starts) > 0 {
		fbb.generateFilter()
	}

	// Append array of per-filter offsets
	buf := bytes.NewBuffer(fbb.filter)
	for _, off := range fbb.filterOffsets {
		PutFixed32(buf, uint32(off))
	}
	PutFixed32(buf, uint32(len(fbb.filter)))
	buf.WriteByte(byte(kFilterBaseLg))
	fbb.filter = buf.Bytes()

	return buf.Bytes()
}

func (fbb *FilterBlockBuilder) generateFilter() {
	numKeys := len(fbb.starts)
	if numKeys == 0 {
		// Fast path if there are no keys for this filter
		fbb.filterOffsets = append(fbb.filterOffsets, len(fbb.filter))
		return
	}

	// Make list of keys from flattened key structure
	var tmpKeys [][]byte                           // policy_->CreateFilter() argument
	fbb.starts = append(fbb.starts, len(fbb.keys)) // Simplify length computation
	for i := 0; i < numKeys; i++ {
		base := fbb.starts[i]
		end := fbb.starts[i+1]
		tmpKeys = append(tmpKeys, fbb.keys[base:end])
	}

	// Generate filter for current set of keys and append to filter.
	fbb.filterOffsets = append(fbb.filterOffsets, len(fbb.filter))
	fbb.policy.CreateFilter(tmpKeys, &fbb.filter)

	fbb.keys = nil
	fbb.starts = nil
}

type FilterBlockReader struct {
	policy     FilterPolicy
	baseLg     uint64 // Encoding parameter (see kFilterBaseLg)
	data       []byte
	offsets    []byte
	numOffsets int
}

func NewFilterBlockReader(policy FilterPolicy, contents []byte) *FilterBlockReader {
	if len(contents) < 5 {
		return nil
	}

	fbr := &FilterBlockReader{}
	fbr.policy = policy
	n := len(contents)
	fbr.baseLg = uint64(contents[n-1])
	buf := bytes.NewBuffer(contents[n-5:])
	if lastWord, err := DecodeFixed32(buf); err != nil {
		debug.Printf("DecodeFixed32 error %v", err)
		return nil
	} else {
		var lastWord int = int(lastWord)
		debug.Printf("lastWord %v", lastWord)
		if lastWord+5 > n {
			return nil
		}

		fbr.data = contents[0:lastWord]
		fbr.offsets = contents[lastWord : n-1]
		fbr.numOffsets = (n - 5 - lastWord) / 4
	}

	return fbr
}

func (fbr *FilterBlockReader) KeyMayMatch(blockOffset uint64, key []byte) bool {
	index := int(blockOffset >> fbr.baseLg)
	if index < fbr.numOffsets {
		buf := bytes.NewBuffer(fbr.offsets[index*4 : (index+2)*4])
		start, err := DecodeFixed32(buf)
		if err != nil {
			debug.Fatalf("DecodeFixed32 failed %v", err)
			return false
		}
		end, err := DecodeFixed32(buf)
		if err != nil {
			debug.Fatalf("DecodeFixed32 failed %v", err)
			return false
		}

		if start < end && int(end) <= len(fbr.data) {
			filter := fbr.data[start:end]
			return fbr.policy.KeyMayMatch(key, filter)
		} else if start == end {
			// Empty filters do not match any keys
			return false
		}
	}

	return true // Errors are treated as potential matches
}
