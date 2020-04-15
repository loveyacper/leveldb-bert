// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE VALUES: they are embedded in the on-disk
// data structures.

type ValueType byte

const (
	TypeDeletion ValueType = 0
	TypeValue              = 1

	// kValueTypeForSeek defines the ValueType that should be passed when
	// constructing a ParsedInternalKey object for seeking to a particular
	// sequence number (since we sort sequence numbers in decreasing order
	// and the value type is embedded as the low 8 bits in the sequence
	// number in internal keys, we need to use the highest-numbered
	// ValueType, not the lowest).
	TypeValueForSeek = 1
)

type SequenceNumber uint64

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
const kMaxSequenceNumber SequenceNumber = SequenceNumber(((uint64(1)) << 56) - 1)

// Implements Comparator.
// A comparator for internal keys that uses a specified comparator for
// the user key portion and breaks ties by decreasing sequence number.
type InternalKeyComparator struct {
	userCmp Comparator
}

func (icmp *InternalKeyComparator) Name() string {
	return "leveldb.InternalKeyComparator"
}

func (icmp *InternalKeyComparator) Compare(key1, key2 []byte) int {
	ukey1 := ExtractUserKey(key1)
	ukey2 := ExtractUserKey(key2)
	cmp := icmp.userCmp.Compare(ukey1, ukey2)
	if cmp != 0 {
		return cmp
	}

	// compare sequence, the bigger sequence should put front
	seq1, err1 := DecodeFixed64(bytes.NewBuffer(key1[len(ukey1):]))
	seq2, err2 := DecodeFixed64(bytes.NewBuffer(key2[len(ukey2):]))
	if err1 != nil || err2 != nil {
		panic("BUG: DecodeFixed64 failed")
	}

	if seq1 > seq2 {
		return -1
	} else if seq1 < seq2 {
		return 1
	}

	return 0
}

func (icmp *InternalKeyComparator) FindShortestSeparator(start *[]byte, limit string) {
	// Needed when construct ldb file
	panic("FindShortestSeparator not implemented")
}

func (icmp *InternalKeyComparator) FindShortSuccessor(key *[]byte) {
	// Needed when construct ldb file
	panic("FindShortSuccessor not implemented")
}

// Returns the user key portion of an internal key.
// internalKey format: user_key + 8bytes_of_sequence_type
func ExtractUserKey(internalKey []byte) []byte {
	if len(internalKey) < 8 {
		panic(fmt.Sprintf("internalKey should at least 8 bytes: %v", len(internalKey)))
	}

	return internalKey[0 : len(internalKey)-8]
}

func packSequenceAndType(seq SequenceNumber, t ValueType) uint64 {
	if seq > kMaxSequenceNumber {
		panic(fmt.Sprintf("Too big seq %v", seq))
	}
	s64 := uint64(seq)
	t64 := uint64(t)
	return (s64 << 8) | t64
}

// A helper class useful for DB::Get()
type LookupKey struct {
	space [256]byte

	memtableKey []byte // len + key + seqtype
	internalKey []byte // key + seqtype
	userKey     []byte // key
}

// Initialize *this for looking up user_key at a snapshot with
// the specified sequence number.
func NewLookupKey(userKey []byte, seq SequenceNumber) *LookupKey {
	lk := &LookupKey{}

	usize := len(userKey)
	needed := usize + binary.MaxVarintLen32 + 8

	var dst []byte
	if needed <= len(lk.space) {
		dst = lk.space[:0]
	} else {
		dst = make([]byte, 0, needed)
	}

	buf := bytes.NewBuffer(dst)
	PutVarint32(buf, uint32(usize+8)) // 8 is for seq+type
	internalStart := buf.Len()
	buf.Write(userKey)
	PutFixed64(buf, packSequenceAndType(seq, TypeValueForSeek))

	dst = buf.Bytes()

	lk.memtableKey = dst[:]
	lk.internalKey = dst[internalStart:]
	lk.userKey = dst[internalStart : len(dst)-8]

	return lk
}

// Return a key suitable for lookup in a MemTable.
func (lk *LookupKey) MemtableKey() []byte {
	return lk.memtableKey
}

// Return an internal key (suitable for passing to an internal iterator)
func (lk *LookupKey) InternalKey() []byte {
	return lk.internalKey
}

// Return the user key
func (lk *LookupKey) UserKey() []byte {
	return lk.userKey
}
