// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"encoding/binary"
	"fmt"
)

type RecordType byte

const (
	// Zero is reserved for preallocated files
	zeroType RecordType = 0

	fullType   = 1
	firstType  = 2
	middleType = 3
	lastType   = 4

	maxRecordType = lastType

	eofType = maxRecordType + 1
	// Returned whenever we find an invalid physical record.
	// Currently there are three situations in which this happens:
	// * The record has an invalid CRC (readPhysicalRecord reports a drop)
	// * The record is a 0-length record (No drop is reported)
	// * The record is below constructor's initial_offset (No drop is reported)
	badRecordType = maxRecordType + 2
)

var trailer []byte = []byte("\x00\x00\x00\x00\x00\x00\x00")

const (
	blockSize int = 32 * 1024

	// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
	logHeaderSize int = 4 + 2 + 1
)

type LogWriter struct {
	dest        WritableFile
	blockOffset int
	// crc32c values for all supported record types.  These are
	// pre-computed to reduce the overhead of computing the crc of the
	// record type stored in the header.
	typeCrc [maxRecordType + 1]uint32

	// simulate writer crash before write record data
	crash bool
}

// Create a writer that will append data to "*dest".
// "*dest" must be initially empty.
// "*dest" must remain live while this Writer is in use.
func NewLogWriter(dest WritableFile) *LogWriter {
	lw := &LogWriter{dest: dest}
	lw.blockOffset = 0

	var i byte = 0
	for ; i < byte(maxRecordType); i++ {
		lw.typeCrc[i] = uint32(CrcValue([]byte{i}))
	}

	lw.crash = false
	return lw
}

func (lw *LogWriter) SetCrash(crash bool) {
	lw.crash = crash
}

func (lw *LogWriter) Sync() Status {
	return lw.dest.Sync()
}

func (lw *LogWriter) AddRecord(slice []byte) Status {
	// Fragment the record if necessary and emit it.  Note that if slice
	// is empty, we still want to iterate once to emit a single zero-length record
	data := slice
	s := NewStatus(OK)
	begin := true
	for len(data) > 0 && s.IsOK() {
		leftover := blockSize - lw.blockOffset
		if leftover < 0 {
			panic(fmt.Sprintf("leftover %d", leftover))
		}
		if leftover < logHeaderSize {
			debug.Printf("add trailer %d bytes\n", leftover)
			lw.dest.Append(trailer[:leftover])
			lw.blockOffset = 0
		}

		if blockSize-lw.blockOffset-logHeaderSize < 0 {
			panic(fmt.Sprintf("blockSize %v, blockOffset %v, logHeader %v", blockSize, lw.blockOffset, logHeaderSize))
		}

		avail := blockSize - lw.blockOffset - logHeaderSize
		fragmentLength := len(data)
		if fragmentLength > avail {
			fragmentLength = avail
		}

		var tp RecordType
		end := (len(data) == fragmentLength)
		if begin && end {
			tp = fullType
		} else if begin {
			tp = firstType
		} else if end {
			tp = lastType
		} else {
			tp = middleType
		}

		debug.Printf("AddRecord fragmentLength %d, len(data) %d, begin %v, end %v\n", fragmentLength, len(data), begin, end)
		s = lw.emitPhysicalRecord(tp, data[:fragmentLength])
		data = data[fragmentLength:]
		begin = false
	}

	return s
}

func (lw *LogWriter) Close() Status {
	if err := lw.dest.Close(); err != nil {
		return NewStatus(IOError, err.Error())
	}

	return NewStatus(OK)
}

func (lw *LogWriter) emitPhysicalRecord(tp RecordType, data []byte) Status {
	// 4 + 2 + 1 + data, data can be zero-length
	var header [logHeaderSize]byte
	binary.LittleEndian.PutUint16(header[4:6], uint16(len(data)))
	header[6] = byte(tp)

	// put crc to header[:4]
	{
		crc := levelDbCrc(lw.typeCrc[tp])
		crc = crc.Extend(data).Mask()
		binary.LittleEndian.PutUint32(header[:4], uint32(crc))
	}

	s := lw.dest.Append(header[:])
	if lw.crash {
		// simulate writer crash before record data.
		s = lw.dest.Flush()
		return s
	}

	if s.IsOK() {
		s = lw.dest.Append(data)
		if s.IsOK() {
			s = lw.dest.Flush()
		}
	}

	lw.blockOffset += logHeaderSize + len(data)
	return s
}
