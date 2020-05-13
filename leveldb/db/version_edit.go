// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
)

type FileMetaData struct {
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

type filedTag int

const (
	comparator filedTag = 1 + iota
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

func (ve *VersionEdit) Decode(src string) Status {
	return NewStatus(OK)
}
