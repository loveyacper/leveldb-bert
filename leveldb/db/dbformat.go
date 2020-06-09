// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Grouping of constants.  We may want to make some of these
// parameters set via options.
const (
	NumLevels int = 7

	// Level-0 compaction is started when we hit this many files.
	L0_CompactionTrigger = 4

	// Soft limit on number of level-0 files.  We slow down writes at this point.
	L0_SlowdownWritesTrigger = 8

	// Maximum number of level-0 files.  We stop writes at this point.
	L0_StopWritesTrigger = 12

	// Maximum level to which a new compacted memtable is pushed if it
	// does not create overlap.  We try to push to level 2 to avoid the
	// relatively expensive level 0=>1 compactions and to avoid some
	// expensive manifest file operations.  We do not push all the way to
	// the largest level since that can generate a lot of wasted disk
	// space if the same key space is being repeatedly overwritten.
	MaxMemCompactLevel = 2

	// Approximate gap in bytes between samples of data read during iteration.
	ReadBytesPeriod = 1048576
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

// key1, key2 is InternalKey
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
	// Attempt to shorten the user portion of the key
	userStart := ExtractUserKey(*start)
	userLimit := ExtractUserKey([]byte(limit))

	tmp := make([]byte, len(*start))
	copy(tmp, userStart)
	icmp.userCmp.FindShortestSeparator(&tmp, string(userLimit))

	if len(tmp) < len(userStart) && icmp.userCmp.Compare(userStart, tmp) < 0 {
		// User key has become shorter physically, but larger logically.
		// Tack on the earliest possible number to the shortened user key.
		buf := bytes.NewBuffer(tmp)
		PutFixed64(buf, packSequenceAndType(kMaxSequenceNumber, TypeValueForSeek))
		tmp = buf.Bytes()

		if icmp.Compare(*start, tmp) >= 0 || icmp.Compare(tmp, []byte(limit)) >= 0 {
			panic("BUG")
		}

		*start = tmp
	}
}

func (icmp *InternalKeyComparator) FindShortSuccessor(key *[]byte) {
	// Needed when construct ldb file
	userKey := ExtractUserKey(*key)

	tmp := make([]byte, len(*key))
	copy(tmp, userKey)
	icmp.userCmp.FindShortSuccessor(&tmp)
	if len(tmp) < len(userKey) && icmp.userCmp.Compare(userKey, tmp) < 0 {
		// User key has become shorter physically, but larger logically.
		// Tack on the earliest possible number to the shortened user key.
		buf := bytes.NewBuffer(tmp)
		PutFixed64(buf, packSequenceAndType(kMaxSequenceNumber, TypeValueForSeek))
		tmp = buf.Bytes()

		if icmp.Compare(*key, tmp) >= 0 {
			panic("BUG")
		}

		*key = tmp
	}
}

// Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an InternalKeyComparator.
type InternalKey struct {
	rep []byte
}

func NewInternalKey(userKey []byte, s SequenceNumber, t ValueType) *InternalKey {
	key := ParsedInternalKey{}
	key.userKey = userKey
	key.sequence = s
	key.tp = t

	ikey := &InternalKey{}
	ikey.rep = AppendInternalKey(key)
	return ikey
}

func (ikey *InternalKey) DecodeFrom(s []byte) {
	if len(s) > 0 {
		ikey.rep = make([]byte, len(s))
		copy(ikey.rep, s)
	}
}

func (ikey *InternalKey) Encode() []byte {
	return ikey.rep
}

func (ikey *InternalKey) UserKey() []byte {
	return ExtractUserKey(ikey.rep)
}

func (ikey *InternalKey) Clear() {
	ikey.rep = ikey.rep[0:0]
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

type ParsedInternalKey struct {
	userKey  []byte
	sequence SequenceNumber
	tp       ValueType
}

func NewParsedInternalKey(userKey []byte, seq SequenceNumber, t ValueType) ParsedInternalKey {
	pikey := ParsedInternalKey{}
	pikey.userKey = userKey
	pikey.sequence = seq
	pikey.tp = t

	return pikey
}

func AppendInternalKey(key ParsedInternalKey) []byte {
	buf := bytes.NewBuffer(nil)
	buf.Write(key.userKey)
	PutFixed64(buf, packSequenceAndType(key.sequence, key.tp))

	return buf.Bytes()
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
