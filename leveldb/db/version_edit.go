// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"fmt"
)

type FileMetaData struct {
	//  AllowedSeeks; // Seeks allowed until compaction
	//allowed_seeks(1 << 30)

	Number   uint64
	FileSize uint64

	//Smallest InternalKey
	//Largest InternalKey
}

type VersionEdit struct {
	Comparator    string
	HasComparator bool

	LogNumber    uint64
	HasLogNumber bool

	NextFileNumber    uint64
	HasNextFileNumber bool

	LastSequence    SequenceNumber
	HasLastSequence bool

	//compact_pointers_;
	//deleted_files_;
	//new_files_;
}

func (ve *VersionEdit) SetComparatorName(name string) {
	ve.Comparator = name
	ve.HasComparator = true
}

func (ve *VersionEdit) SetLogNumber(num uint64) {
	ve.LogNumber = num
	ve.HasLogNumber = true
}

func (ve *VersionEdit) SetNextFile(num uint64) {
	ve.NextFileNumber = num
	ve.HasNextFileNumber = true
}

func (ve *VersionEdit) SetLastSequence(seq SequenceNumber) {
	ve.LastSequence = seq
	ve.HasLastSequence = true
}

func (ve *VersionEdit) Clear() {
	ve.HasComparator = false
	ve.HasLogNumber = false
	ve.HasNextFileNumber = false
	ve.HasLastSequence = false
}

type fieldTag int

const (
	comparator fieldTag = 1 + iota
	logNumber
	nextFileNumber
	lastSequence
	compactPointer
	deletedFile
	newFile
)

func (ve *VersionEdit) Encode() []byte {
	dst := bytes.NewBuffer([]byte{})
	if ve.HasComparator {
		PutVarint32(dst, uint32(comparator))
		PutLengthPrefixedSlice(dst, []byte(ve.Comparator))
	}
	if ve.HasLogNumber {
		PutVarint32(dst, uint32(logNumber))
		PutVarint64(dst, uint64(ve.LogNumber))
	}
	if ve.HasNextFileNumber {
		PutVarint32(dst, uint32(nextFileNumber))
		PutVarint64(dst, uint64(ve.NextFileNumber))
	}
	if ve.HasLastSequence {
		PutVarint32(dst, uint32(lastSequence))
		PutVarint64(dst, uint64(ve.LastSequence))
	}

	return dst.Bytes()
}

func (ve *VersionEdit) Decode(input []byte) Status {
	ve.Clear()

	st := NewStatus(OK)
	src := bytes.NewBuffer(input)
	for st.IsOK() {
		if tp, err := GetVarint32(src); err != nil {
			break
		} else {
			debug.Println("got type ", tp)
			switch fieldTag(tp) {
			case comparator:
				var cmp []byte
				if err := GetLengthPrefixedSlice(src, &cmp); err != nil {
					st = NewStatus(IOError, err.Error())
				} else {
					ve.SetComparatorName(string(cmp))
				}
			case logNumber:
				if n, err := GetVarint64(src); err != nil {
					st = NewStatus(IOError, "logNumber", err.Error())
				} else {
					ve.SetLogNumber(n)
				}
			case nextFileNumber:
				if n, err := GetVarint64(src); err != nil {
					st = NewStatus(IOError, "nextFileNumber", err.Error())
				} else {
					ve.SetNextFile(n)
				}
			case lastSequence:
				if n, err := GetVarint64(src); err != nil {
					st = NewStatus(IOError, "lastSequence", err.Error())
				} else {
					ve.SetLastSequence(SequenceNumber(n))
				}
			default:
				panic(fmt.Sprintf("version edit: unknow type %v", tp))
			}
		}
	}

	return st
}
