package leveldb

// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

import (
	"bytes"
)

// A Comparator object provides a total order across slices that are
// used as keys in an sstable or a database.  A Comparator implementation
// must be thread-safe since leveldb may invoke its methods concurrently
// from multiple threads.
type Comparator interface {
	// Three-way comparison.  Returns value:
	//   < 0 iff "a" < "b",
	//   == 0 iff "a" == "b",
	//   > 0 iff "a" > "b"
	Compare(key1, key2 []byte) int

	// The name of the comparator.  Used to check for comparator
	// mismatches (i.e., a DB created with one comparator is
	// accessed using a different comparator.
	//
	// The client of this package should switch to a new name whenever
	// the comparator implementation changes in a way that will cause
	// the relative ordering of any two keys to change.
	//
	// Names starting with "leveldb." are reserved and should not be used
	// by any clients of this package.
	Name() string
	// Advanced functions: these are used to reduce the space requirements
	// for internal data structures like index blocks.

	// If *start < limit, changes *start to a short string in [start,limit).
	// Simple comparator implementations may return with *start unchanged,
	// i.e., an implementation of this method that does nothing is correct.
	FindShortestSeparator(start *[]byte, limit string)

	// Changes *key to a short string >= *key.
	// Simple comparator implementations may return with *key unchanged,
	// i.e., an implementation of this method that does nothing is correct.
	FindShortSuccessor(key *[]byte)
}

// Return a builtin comparator that uses lexicographic byte-wise
// ordering.  The result remains the property of this module and
// must not be deleted.
func NewBytewiseComparator() Comparator {
	return &BytewiseComparatorImpl{}
}

type BytewiseComparatorImpl struct{}

func (dummy *BytewiseComparatorImpl) Name() string {
	return "leveldb.BytewiseComparator"
}

func (dummy *BytewiseComparatorImpl) Compare(key1, key2 []byte) int {
	if key1 == nil && key2 == nil {
		panic("both key can't be nil")
	} else if key1 == nil {
		return 1
	} else if key2 == nil {
		return -1
	}

	return bytes.Compare(key1, key2)
}

func (dummy *BytewiseComparatorImpl) FindShortestSeparator(start *[]byte, limit string) {
	// Find length of common prefix
	min_len := len(*start)
	if min_len > len(limit) {
		min_len = len(limit)
	}
	diff_index := 0
	for diff_index < min_len && (*start)[diff_index] == limit[diff_index] {
		diff_index++
	}

	if diff_index >= min_len {
		// Do not shorten if one string is a prefix of the other
	} else {
		diff_byte := (*start)[diff_index]
		if diff_byte < byte(0xFF) && diff_byte+1 < byte(limit[diff_index]) {
			(*start)[diff_index] += 1
			(*start) = (*start)[:diff_index+1]
			if dummy.Compare(*start, []byte(limit)) >= 0 {
				panic("BUG")
			}
		}
	}
}

func (dummy *BytewiseComparatorImpl) FindShortSuccessor(key *[]byte) {
	// Find first character that can be incremented
	n := len(*key)
	for i := 0; i < n; i++ {
		v := (*key)[i]
		if v != byte(0xFF) {
			(*key)[i] = v + 1
			(*key) = (*key)[:i+1]
			return
		}
	}

	// *key is a run of 0xffs.  Leave it alone.
}
