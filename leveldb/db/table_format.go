// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"github.com/golang/snappy"
)

const (
	kMaxBlockHandleEncodedLength = 10 + 10 // Maximum encoding length of a BlockHandle

	// Encoded length of a Footer.  Note that the serialization of a
	// Footer will always occupy exactly this many bytes.  It consists
	// of two block handles and a magic number.
	kFooterEncodedLength = 2*kMaxBlockHandleEncodedLength + 8
)

// BlockHandle is a pointer to the extent of a file that stores a data
// block or a meta block.
type BlockHandle struct {
	Offset uint64 // The offset of the block in the file.
	Size   uint64 // The size of the stored block
}

func (bh *BlockHandle) EncodeTo() []byte {
	buf := bytes.NewBuffer(nil)
	bh.EncodeToBuf(buf)
	return buf.Bytes()
}

func (bh *BlockHandle) EncodeToBuf(buf *bytes.Buffer) {
	PutVarint64(buf, bh.Offset)
	PutVarint64(buf, bh.Size)
}

func (bh *BlockHandle) DecodeFrom(input []byte) Status {
	buf := bytes.NewBuffer(input)
	return bh.DecodeFromBuf(buf)
}

func (bh *BlockHandle) DecodeFromBuf(buf *bytes.Buffer) Status {
	if v, err := GetVarint64(buf); err != nil {
		return NewStatus(Corruption, "(Offset) bad block handle")
	} else {
		bh.Offset = v
	}
	if v, err := GetVarint64(buf); err != nil {
		return NewStatus(Corruption, "(Size) bad block handle")
	} else {
		bh.Size = v
	}

	return NewStatus(OK)
}

// Footer encapsulates the fixed information stored at the tail
// end of every table file.
type Footer struct {
	MetaIndex BlockHandle // The block handle for the metaindex block of the table
	DataIndex BlockHandle // The block handle for the index block of the table
}

func (f *Footer) EncodeTo() []byte {
	buf := bytes.NewBuffer(nil)
	f.MetaIndex.EncodeToBuf(buf)
	f.DataIndex.EncodeToBuf(buf)

	padding := 2*kMaxBlockHandleEncodedLength - len(buf.Bytes())
	if padding > 0 {
		buf.Write(make([]byte, padding))
	}

	PutFixed32(buf, uint32(kTableMagicNumber&uint64(0x00ffffffff)))
	PutFixed32(buf, uint32(kTableMagicNumber>>32))
	return buf.Bytes()
}

func (f *Footer) DecodeFrom(input []byte) Status {
	buf := bytes.NewBuffer(input)
	return f.DecodeFromBuf(buf)
}

func (f *Footer) DecodeFromBuf(input *bytes.Buffer) Status {
	oldLen := len(input.Bytes())
	if oldLen < kFooterEncodedLength {
		return NewStatus(Corruption, "Shorten footer")
	}

	{
		magicBytes := input.Bytes()[kFooterEncodedLength-8 : kFooterEncodedLength]
		magicBuf := bytes.NewBuffer(magicBytes)
		magicLo, _ := DecodeFixed32(magicBuf)
		magicHi, _ := DecodeFixed32(magicBuf)
		var magic uint64 = (uint64(magicHi) << 32) | uint64(magicLo)
		if magic != kTableMagicNumber {
			return NewStatus(Corruption, "not an sstable (bad magic number)")
		}
	}

	if st := f.MetaIndex.DecodeFromBuf(input); !st.IsOK() {
		return st
	}
	if st := f.DataIndex.DecodeFromBuf(input); !st.IsOK() {
		return st
	}

	newLen := len(input.Bytes())
	toSkip := kFooterEncodedLength - (newLen - oldLen)
	input.Next(toSkip)

	return NewStatus(OK)
}

// kTableMagicNumber was picked by running
//    echo http://code.google.com/p/leveldb/ | sha1sum
// and taking the leading 64 bits.
const (
	kTableMagicNumber uint64 = 0xdb4775248b80fb57
	kBlockTrailerSize uint64 = 5 // 1-byte type + 32-bit crc
)

type BlockContents struct {
	Data     []byte // Actual contents of data
	Cachable bool   // True iff data can be cached
}

// Read the block identified by "handle" from "file".  On failure
// return non-OK.  On success return BlockContents and OK.
func ReadBlock(file RandomAccessFile, verifyCrc bool, handle BlockHandle) (BlockContents, Status) {
	// Read the block contents as well as the type/crc footer.
	// See table_builder.go for the code that built this structure.
	n := handle.Size
	buf := make([]byte, n+kBlockTrailerSize)
	contents, st := file.Read(int64(handle.Offset), len(buf), buf)
	if !st.IsOK() {
		return BlockContents{}, st
	}

	if len(contents) != len(buf) {
		return BlockContents{}, NewStatus(Corruption, "truncated block read")
	}

	// Check the crc of the type and the block contents
	if verifyCrc {
		dst := bytes.NewBuffer(contents[n+1:])
		crc, _ := DecodeFixed32(dst)
		expectCrc := CrcValue(contents[0 : n+1]).Mask()
		if crc != uint32(expectCrc) {
			return BlockContents{}, NewStatus(Corruption, "block checksum mismatch")
		}
	}

	switch contents[n] {
	case kNoCompression:
		bc := BlockContents{}
		bc.Data = buf[:n]
		return bc, NewStatus(OK)

	case kSnappyCompression:
		bc := BlockContents{}
		if data, err := snappy.Decode(nil, buf[:n]); err != nil {
			return bc, NewStatus(Corruption, "corrupted compressed block contents")
		} else {
			bc.Data = data
		}
		return bc, NewStatus(OK)

	default:
		return BlockContents{}, NewStatus(Corruption, "bad block type")
	}

	return BlockContents{}, NewStatus(OK)
}
