// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	//"bytes"
	"encoding/binary"
	"fmt"
	"log"
)

type LogReader struct {
	src         SequentialFile
	blockOffset int
	checkCrc    bool
	eof         bool

	record       []byte
	backingStore [blockSize]byte // A physical record
	// crc32c values for all supported record types.  These are
	// pre-computed to reduce the overhead of computing the crc of the
	// record type stored in the header.
	typeCrc [maxRecordType + 1]uint32
}

// Create a reader that will return log records from "*file".
// "*file" must remain live while this Reader is in use.
//
// If "reporter" is non-NULL, it is notified whenever some data is
// dropped due to a detected corruption.  "*reporter" must remain
// live while this Reader is in use.
//
// If "checksum" is true, verify checksums if available.
//
// The Reader will start reading at the first record located at physical
// position >= initial_offset within the file.
func NewLogReader(src SequentialFile) *LogReader {
	lr := &LogReader{src: src}
	lr.blockOffset = 0
	lr.checkCrc = true
	lr.eof = false
	lr.record = lr.backingStore[0:0]

	var i byte = 0
	for ; i < byte(maxRecordType); i++ {
		lr.typeCrc[i] = uint32(CrcValue([]byte{i}))
	}

	return lr
}

func (lr *LogReader) EnableCheckCrc(enable bool) {
	lr.checkCrc = enable
}

// Read the next record into *record.  Returns true if read
// successfully, false if we hit end of the input.  May use
// "*scratch" as temporary storage.  The contents filled in *record
// will only be valid until the next mutating operation on this
// reader or the next mutation to *scratch.
func (lr *LogReader) ReadRecord(record *[]byte) bool {
	for {
		rec, tp := lr.readPhysicalRecord()
		if rec == nil {
			return false
		}

		switch tp {
		case fullType:
			*record = rec // shallow copy
			return true
		case firstType:
			if record != nil {
				*record = make([]byte, 0, 64)
			}
			*record = append(*record, rec...)
		case middleType:
			*record = append(*record, rec...)
		case lastType:
			*record = append(*record, rec...)
			return true
		default:
			panic(fmt.Sprintf("unknown type %v", tp))
		}
	}

	return false
}

func (lr *LogReader) Close() Status {
	if err := lr.src.Close(); err != nil {
		return NewStatus(IOError, err.Error())
	}

	return NewStatus(OK)
}

func (lr *LogReader) readPhysicalRecord() ([]byte, RecordType) {
	if lr.eof {
		return nil, eofType
	}

	if len(lr.record) < logHeaderSize {
		if len(lr.record) > 0 {
			// Last read was a full read, so this is a trailer to skip
			log.Printf("skip trailer %v", len(lr.record))
		}

		lr.record = lr.backingStore[0:0]
		if data, st := lr.src.Read(blockSize, lr.backingStore[:]); !st.IsOK() {
			log.Println("eof...")
			lr.eof = true
			return nil, eofType
		} else {
			if len(data) < blockSize {
				log.Println("eof not enough data...")
				lr.eof = true
			}

			lr.record = lr.backingStore[0:len(data)]
		}
	}

	// read header
	var dataLen int = int(binary.LittleEndian.Uint16(lr.record[4:6]))
	if dataLen < 0 {
		panic(fmt.Sprintf("Wrong dataLen %d", dataLen))
	}

	if len(lr.record) < dataLen+logHeaderSize {
		// something abnormal, not enough data
		if lr.eof {
			// If the end of the file has been reached without reading |length| bytes
			// of payload, assume the writer died in the middle of writing the record.
			// Don't report a corruption.
			log.Println("writer crash, skip this record")
			return nil, eofType
		}

		return nil, badRecordType
	}

	tp := RecordType(lr.record[6])
	if lr.checkCrc {
		crc := binary.LittleEndian.Uint32(lr.record[:4])
		realCrc := levelDbCrc(lr.typeCrc[tp])
		realCrc = realCrc.Extend(lr.record[logHeaderSize : logHeaderSize+dataLen])
		if crc != uint32(realCrc) {
			return nil, badRecordType
		}
	}

	rec := lr.record[logHeaderSize : logHeaderSize+dataLen]
	// skip rec
	lr.record = lr.record[logHeaderSize+dataLen:]
	return rec, tp
}
