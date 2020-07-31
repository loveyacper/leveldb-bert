// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"fmt"
	"strconv"
)

type FileMetaData struct {
	//  AllowedSeeks; // Seeks allowed until compaction
	//allowed_seeks(1 << 30)

	Level    int
	Number   uint64
	FileSize uint64

	Smallest InternalKey
	Largest  InternalKey
}

func (f *FileMetaData) String() string {
	buf := new(bytes.Buffer)
	buf.WriteString("\n[")
	buf.WriteString("Level:" + strconv.Itoa(f.Level))
	buf.WriteString(" Number:" + strconv.Itoa(int(f.Number)))
	buf.WriteString(" Size:" + strconv.Itoa(int(f.FileSize)))
	buf.WriteString(" Smallest:" + string(f.Smallest.Encode()))
	buf.WriteString(" Largest:" + string(f.Largest.Encode()))
	buf.WriteString("]\n")

	return string(buf.Bytes())
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

	//vector<pair<int, InternalKey> > compact_pointers_;
	// level --> number
	//typedef std::set< std::pair<int, uint64_t> > DeletedFileSet;
	//deleted_files_;

	DeletedFiles []FileMetaData
	NewFiles     []FileMetaData
}

func (ve *VersionEdit) String() string {
	buf := new(bytes.Buffer)
	buf.WriteString("\n[")
	if ve.HasComparator {
		buf.WriteString("Comparator:" + ve.Comparator)
	}
	if ve.HasLogNumber {
		buf.WriteString(" LogNumber:" + strconv.Itoa(int(ve.LogNumber)))
	}
	if ve.HasNextFileNumber {
		buf.WriteString(" NextFileNumber:" + strconv.Itoa(int(ve.NextFileNumber)))
	}
	if ve.HasLastSequence {
		buf.WriteString(" LastSequence:" + strconv.Itoa(int(ve.LastSequence)))
	}

	buf.WriteString("\nDeleteFiles: ")
	for _, f := range ve.DeletedFiles {
		buf.WriteString(f.String())
	}
	buf.WriteString("\nNewFiles: ")
	for _, f := range ve.NewFiles {
		buf.WriteString(f.String())
	}
	buf.WriteString("]\n")
	return string(buf.Bytes())
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

	ve.DeletedFiles = nil
	ve.NewFiles = nil
}

// Add the specified file at the specified number.
// REQUIRES: This version has not been saved (see VersionSet::SaveTo)
// REQUIRES: "smallest" and "largest" are smallest and largest keys in file
func (ve *VersionEdit) AddFile(level int, fileNumber, fileSize uint64, smallest, largest InternalKey) {
	var f FileMetaData
	f.Level = level
	f.Number = fileNumber
	f.FileSize = fileSize
	f.Smallest = smallest
	f.Largest = largest

	ve.NewFiles = append(ve.NewFiles, f)
}

// Delete the specified "file" from the specified "level".
func (ve *VersionEdit) DeleteFile(level int, fileNumber uint64) {
	var f FileMetaData
	f.Level = level
	f.Number = fileNumber

	ve.DeletedFiles = append(ve.DeletedFiles, f)
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

	for _, f := range ve.DeletedFiles {
		PutVarint32(dst, uint32(deletedFile))
		PutVarint32(dst, uint32(f.Level))
		PutVarint64(dst, f.Number)
	}

	for _, f := range ve.NewFiles {
		PutVarint32(dst, uint32(newFile))
		PutVarint32(dst, uint32(f.Level))
		PutVarint64(dst, f.Number)
		PutVarint64(dst, f.FileSize)
		PutLengthPrefixedSlice(dst, f.Smallest.Encode())
		PutLengthPrefixedSlice(dst, f.Largest.Encode())
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
			case deletedFile:
				if l, err := GetVarint32(src); err != nil {
					st = NewStatus(IOError, "deletedFile level error ", err.Error())
				} else {
					if n, err := GetVarint64(src); err != nil {
						st = NewStatus(IOError, "deletedFile number error ", err.Error())
					} else {
						ve.DeleteFile(int(l), n)
					}
				}
			case newFile:
				var level uint32
				var number uint64
				var fsize uint64
				var small, large InternalKey
				var err error

				if level, err = GetVarint32(src); err != nil {
					return NewStatus(IOError, "newFile level error ", err.Error())
				}

				if number, err = GetVarint64(src); err != nil {
					return NewStatus(IOError, "newFile number error ", err.Error())
				}

				if fsize, err = GetVarint64(src); err != nil {
					return NewStatus(IOError, "newFile filesize error ", err.Error())
				}

				var key []byte
				if err = GetLengthPrefixedSlice(src, &key); err != nil {
					return NewStatus(IOError, "newFile internal key error ", err.Error())
				} else {
					small.DecodeFrom(key)
				}
				if err = GetLengthPrefixedSlice(src, &key); err != nil {
					return NewStatus(IOError, "newFile internal key error ", err.Error())
				} else {
					large.DecodeFrom(key)
				}

				ve.AddFile(int(level), number, fsize, small, large)
			default:
				panic(fmt.Sprintf("version edit: unknow type %v", tp))
			}
		}
	}

	debug.Println("Decode versionEdit", ve.String())
	return st
}
