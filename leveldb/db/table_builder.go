// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
//
// TableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a TableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same TableBuilder must use
// external synchronization.

package leveldb

import (
	"bytes"
	"fmt"
	"github.com/golang/snappy"
)

type TableBuilder struct {
	options       *Options
	file          WritableFile
	filterBuilder *FilterBlockBuilder

	offset uint64
	status Status

	dataBuilder      *BlockBuilder
	dataIndexBuilder *BlockBuilder
	lastKey          []byte
	numEntries       uint64

	closed bool

	// We do not emit the index entry for a block until we have seen the
	// first key for the next data block.  This allows us to use shorter
	// keys in the index block.  For example, consider a block boundary
	// between the keys "the quick brown fox" and "the who".  We can use
	// "the r" as the key for the index block entry since it is >= all
	// entries in the first block and < all entries in subsequent
	// blocks.
	//
	// Invariant: pendingHandle is not nil only if data_block is empty.
	pendingHandle *BlockHandle // Handle to add to index block
}

func NewTableBuilder(opts *Options, file WritableFile) *TableBuilder {
	tb := &TableBuilder{}
	tb.status = NewStatus(OK)

	tb.options = opts
	tb.file = file
	if opts.Policy != nil {
		tb.filterBuilder = &FilterBlockBuilder{}
		tb.filterBuilder.policy = opts.Policy
	}

	tb.dataBuilder = NewBlockBuilder()
	tb.dataIndexBuilder = NewBlockBuilder()
	tb.closed = false

	return tb
}

func (tb *TableBuilder) Add(key, value []byte) {
	if tb.closed {
		panic("closed when TableBuilder.Add")
	}

	if !tb.status.IsOK() {
		return
	}

	if tb.numEntries > 0 {
		// New key must be greater than exist key, because skiplist is ordered
		if tb.options.Comp.Compare(key, tb.lastKey) <= 0 {
			panic(fmt.Sprintf("key is not ordered: %v <= %v\n", key, tb.lastKey))
		}
	}

	if tb.pendingHandle != nil {
		if !tb.dataBuilder.Empty() {
			// 上一个数据块一定被写入文件了！
			panic("pendingHandle is nil but dataBuilder is not empty!")
		}
		// 之前写入了一个数据块，但是对应的pending_handle还没有写入
		// 之所以这样，是因为两个连续的数据块A和B是有序的，index-A的key，必须>= A.biggest_key, < B.smallest_key，亦即现在参数key
		// leveldb这样写是为了优化，万一key之间有公共前缀，可以节约一点字节
		// 但是现在需要记录它的index-handle，为啥拖到现在才写？举个例子：假设上一个数据块最大的key是shandong,
		// 现在这个新数据块第一个key是shanghai；我们要确定一个key，它>= shandong，但是< shanghai；
		// 可以计算出这个key是 shane
		tb.options.Comp.FindShortestSeparator(&tb.lastKey, string(key))
		encoding := tb.pendingHandle.EncodeTo()
		tb.dataIndexBuilder.Add(tb.lastKey, encoding) // index block中有很多 block handle
		tb.pendingHandle = nil
	}

	if tb.filterBuilder != nil {
		tb.filterBuilder.AddKey(key)
	}

	tb.lastKey = key
	tb.numEntries++
	tb.dataBuilder.Add(key, value)

	if tb.dataBuilder.CurrentSizeEstimate() > int(tb.options.BlockSize) {
		tb.Flush()
	}
}

func (tb *TableBuilder) Flush() {
	if tb.closed {
		panic("closed when TableBuilder.Flush")
	}

	if !tb.status.IsOK() {
		return
	}

	if tb.dataBuilder.Empty() {
		return
	}

	if tb.pendingHandle != nil {
		panic("pendingHandle is not nil when TableBuilder.Flush()")
	}

	handle := tb.writeBlock(tb.dataBuilder)
	tb.pendingHandle = &handle // 写入了一个数据块，但index(pendingHandle)还没写入

	if tb.status.IsOK() {
		tb.status = tb.file.Flush() // 机器挂掉还是可能丢数据的,丢了也无所谓吧..反正该文件还没记录到manifest
	}

	if tb.filterBuilder != nil {
		tb.filterBuilder.StartBlock(tb.offset)
	}
}

func (tb *TableBuilder) writeBlock(block *BlockBuilder) BlockHandle {
	data := block.Finish()

	switch tb.options.CompressionType {
	case kNoCompression:

	case kSnappyCompression:
		data = snappy.Encode(nil, data)
	default:
		panic("invalid compress")
	}

	handle := tb.writeRawBlock(data, tb.options.CompressionType)
	block.Reset()

	return handle
}

func (tb *TableBuilder) writeRawBlock(data []byte, compressType byte) BlockHandle {
	handle := BlockHandle{}
	handle.Offset = tb.offset
	handle.Size = uint64(len(data))

	tb.status = tb.file.Append(data)
	if tb.status.IsOK() {
		trailer := [kBlockTrailerSize]byte{} // type + crc
		trailer[0] = compressType
		crc := CrcValue(data)
		crc = crc.Extend(trailer[:1]).Mask() // Extend crc to cover block type

		buf := bytes.NewBuffer(trailer[1:])
		PutFixed32(buf, uint32(crc))
		tb.status = tb.file.Append(trailer[:])
		if tb.status.IsOK() {
			tb.offset += uint64(len(data) + len(trailer))
		}
	}

	return handle
}

func (tb *TableBuilder) Abandon() {
	if tb.closed {
		panic("already closed TableBuilder")
	}

	tb.closed = true
}

func (tb *TableBuilder) NumEntries() uint64 {
	return tb.numEntries
}

func (tb *TableBuilder) FileSize() uint64 {
	return tb.offset
}

func (tb *TableBuilder) Finish() Status {
	if tb.closed {
		panic("already closed TableBuilder.Finish")
	}
	tb.Flush()
	tb.closed = true

	if !tb.status.IsOK() {
		return tb.status
	}

	var filterHandle BlockHandle
	var metaIndexHandle BlockHandle
	var dataIndexHandle BlockHandle
	// Write filter block
	if tb.filterBuilder != nil {
		filterHandle = tb.writeRawBlock(tb.filterBuilder.Finish(), kNoCompression)
	}

	// Write metaindex block
	if tb.status.IsOK() {
		metaIndexBlock := NewBlockBuilder()
		if tb.filterBuilder != nil {
			// Add mapping from "filter.Name" to location of filter data
			key := []byte("filter.")
			key = append(key, tb.options.Policy.Name()...)
			encoding := filterHandle.EncodeTo()
			metaIndexBlock.Add(key, encoding)
		}

		// TODO(postrelease): Add stats and other meta blocks
		metaIndexHandle = tb.writeBlock(metaIndexBlock)
	}

	// Write index data block
	if tb.status.IsOK() {
		if tb.pendingHandle != nil {
			tb.options.Comp.FindShortSuccessor(&tb.lastKey)
			encoding := tb.pendingHandle.EncodeTo()
			tb.dataIndexBuilder.Add(tb.lastKey, encoding)
			tb.pendingHandle = nil
		}
		dataIndexHandle = tb.writeBlock(tb.dataIndexBuilder)
	}

	// Write footer
	if tb.status.IsOK() {
		var foot Footer
		foot.MetaIndex = metaIndexHandle
		foot.DataIndex = dataIndexHandle
		encoding := foot.EncodeTo()
		tb.status = tb.file.Append(encoding)
		if tb.status.IsOK() {
			tb.status = tb.file.Sync()
			tb.offset += uint64(len(encoding))
		}
	}

	return tb.status
}
