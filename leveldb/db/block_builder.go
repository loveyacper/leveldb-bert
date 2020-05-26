// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

package leveldb

import (
	"bytes"
	"unsafe"
)

const (
	kMaxRestartLen = 16       // or get it from option
	kBlockSize     = 4 * 1024 // or get it from option
)

type BlockBuilder struct {
	data []byte // k,v data, may be compressed

	lastKey []byte

	restarts   []int32
	restartLen int // Number of entries emitted since restart

	finished bool
}

func NewBlockBuilder() *BlockBuilder {
	bb := &BlockBuilder{}
	bb.Reset()
	return bb
}

func (bb *BlockBuilder) Reset() {
	bb.restarts = make([]int32, 1)
	bb.restarts[0] = 0

	bb.restartLen = 0
	bb.lastKey = nil
	bb.data = nil

	bb.finished = false
}

// REQUIRES: Finish() has not been called since the last call to Reset().
// REQUIRES: key is larger than any previously added key
func (bb *BlockBuilder) Add(key, value []byte) {
	shared := 0
	if bb.restartLen == kMaxRestartLen {
		bb.restarts = append(bb.restarts, int32(len(bb.data)))
		bb.restartLen = 0
	} else {
		for i := 0; i < len(bb.lastKey) && i < len(key); i++ {
			if bb[i] != key[i] {
				break
			}
			shared++
		}
	}

	nonShared := len(key) - shared

	// Add "<shared><non_shared><value_size>" to data[]
	buf := bytes.NewBuffer(bb.data)
	PutVarint32(buf, uint32(shared))
	PutVarint32(buf, uint32(nonShared))
	PutVarint32(buf, uint32(len(value)))
	buf.Write(key[shared:])
	buf.Write(value)

	bb.data = buf.Bytes()
	bb.lastKey = key
	bb.restartLen++
}

func (bb *BlockBuilder) CurrentSizeEstimate() int {
	return len(bb.data) + len(bb.restarts)*unsafe.Sizeof(int32) + unsafe.Sizeof(int32)
}

func (bb *BlockBuilder) Finish() []byte {
	if bb.finished {
		panic("Already finished")
	}

	// Format: kv-pairs + restart-int-array + array_size

	// Append restart array
	buf := bytes.NewBuffer(bb.data)
	for i := 0; i < len(bb.restarts); i++ {
		PutFixed32(buf, uint32(bb.restarts[i]))
	}
	PutFixed32(buf, uint32(len(bb.restarts)))

	bb.finished = true
	return buf.Bytes()
}
