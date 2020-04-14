// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

// An iterator yields a sequence of key/value pairs from a source.
// The following class defines the interface.  Multiple implementations
// are provided by this library.  In particular, iterators are provided
// to access the contents of a Table or a DB.

type Iterator interface {
	// An iterator is either positioned at a key/value pair, or
	// not valid.  This method returns true iff the iterator is valid.
	Valid() bool

	// Position at the first key in the source.  The iterator is Valid()
	// after this call iff the source is not empty.
	SeekToFirst()

	// Position at the last key in the source.  The iterator is
	// Valid() after this call iff the source is not empty.
	SeekToLast()

	// Position at the first key in the source that at or past target
	// The iterator is Valid() after this call iff the source contains
	// an entry that comes at or past target.
	Seek(target []byte)

	// Moves to the next entry in the source.  After this call, Valid() is
	// true iff the iterator was not positioned at the last entry in the source.
	// REQUIRES: Valid()
	Next()

	// Moves to the previous entry in the source.  After this call, Valid() is
	// true iff the iterator was not positioned at the first entry in source.
	// REQUIRES: Valid()
	Prev()

	// Return the key for the current entry.  The underlying storage for
	// the returned slice is valid only until the next modification of
	// the iterator.
	// REQUIRES: Valid()
	Key() []byte

	// Return the value for the current entry.  The underlying storage for
	// the returned slice is valid only until the next modification of
	// the iterator.
	// REQUIRES: Valid()
	Value() []byte

	// If an error has occurred, return it.  Else return an ok status.
	Status() Status
}

type EmptyIterator struct {
	state Status
}

// Return an empty iterator with the specified status.
func NewEmptyIterator(s Status) Iterator {
	return &EmptyIterator{state: s}
}

func (_ *EmptyIterator) Valid() bool {
	return false
}

func (_ *EmptyIterator) Seek([]byte) {
}

func (_ *EmptyIterator) SeekToFirst() {
}

func (_ *EmptyIterator) SeekToLast() {
}

func (_ *EmptyIterator) Prev() {
	panic("Can't Prev")
}

func (_ *EmptyIterator) Next() {
	panic("Can't Next")
}

func (_ *EmptyIterator) Key() []byte {
	panic("Can't have key")
	return nil
}

func (_ *EmptyIterator) Value() []byte {
	panic("Can't have value")
	return nil
}

func (i *EmptyIterator) Status() Status {
	return i.state
}
