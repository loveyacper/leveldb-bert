// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"encoding/binary"
)

type MemtableKeyComparator struct {
	*InternalKeyComparator
}

func NewMemtableKeyComparator(icmp *InternalKeyComparator) *MemtableKeyComparator {
	mcmp := &MemtableKeyComparator{}
	mcmp.InternalKeyComparator = icmp
	return mcmp
}

func (mcmp *MemtableKeyComparator) Name() string {
	return "leveldb.MemtableKeyComparator"
}

func (mcmp *MemtableKeyComparator) Compare(key1, key2 []byte) int {
	// key1 key2 format: var_len + key + seq_type
	if key1 == nil && key2 == nil {
		panic("Can't have both nil keys")
	}

	if key1 == nil {
		return 1
	}

	if key2 == nil {
		return -1
	}

	input1 := bytes.NewBuffer(key1)
	input2 := bytes.NewBuffer(key2)

	len1, err1 := GetVarint32(input1)
	if err1 != nil {
		debug.Panicf("GetVarint32 for %v failed", key1)
	}
	if input1.Len() < int(len1) {
		debug.Panicf("input1 len %v, expect len %v", input1.Len(), len1)
	}

	len2, err2 := GetVarint32(input2)
	if err2 != nil {
		debug.Panicf("GetVarint32 for %v failed", key2)
	}
	if input2.Len() < int(len2) {
		debug.Panicf("input2 len %v, expect len %v", input2.Len(), len2)
	}

	internalKey1 := input1.Bytes()
	internalKey2 := input2.Bytes()
	return mcmp.InternalKeyComparator.Compare(internalKey1[:len1], internalKey2[:len2])
}

type MemTable struct {
	mcmp  *MemtableKeyComparator
	table *SkipList
}

func NewMemtable(cmp *InternalKeyComparator) *MemTable {
	tbl := &MemTable{}
	tbl.mcmp = &MemtableKeyComparator{cmp}
	tbl.table = NewSkipList(tbl.mcmp)
	return tbl
}

// Returns an estimate of the number of bytes of data in use by this
// data structure.
//
// REQUIRES: external synchronization to prevent simultaneous
// operations on the same MemTable.
func (mtbl *MemTable) ApproximateMemoryUsage() uint64 {
	return mtbl.table.ByteSize()
}

// Return an iterator that yields the contents of the memtable.
//
// The caller must ensure that the underlying MemTable remains live
// while the returned iterator is live.  The keys returned by this
// iterator are internal keys encoded by AppendInternalKey in the
// db/format.{h,cc} module.
func (mtbl *MemTable) NewIterator() Iterator {
	return NewMemTableIterator(mtbl)
}

// Add an entry into memtable that maps key to value at the
// specified sequence number and with the specified type.
// Typically value will be empty if type==kTypeDeletion.
func (mtbl *MemTable) Add(seq SequenceNumber, t ValueType, key, value []byte) {
	// Format of an entry is concatenation of:
	//  key_size     : varint32 of internal_key.size()
	//  key bytes    : char[internal_key.size()] include seq+type
	//  value_size   : varint32 of value.size()
	//  value bytes  : char[value.size()]
	keySize := len(key)
	valSize := len(value)

	var buf [binary.MaxVarintLen64]byte
	varKeyLen := binary.PutUvarint(buf[:], uint64(keySize)+8)
	varValLen := binary.PutUvarint(buf[:], uint64(valSize))

	keyBuf := new(bytes.Buffer)
	keyBuf.Grow(varKeyLen + keySize + 8 + varValLen + valSize)

	PutVarint32(keyBuf, uint32(keySize+8))
	keyBuf.Write(key)
	PutFixed64(keyBuf, packSequenceAndType(seq, t))
	PutVarint32(keyBuf, uint32(valSize))
	keyBuf.Write(value)

	mtbl.table.Insert(keyBuf.Bytes(), nil)
}

// If memtable contains a value for key, store it in *value and return true.
// If memtable contains a deletion for key, store a NotFound() error
// in *status and return true.
// Else, return false.
func (mtbl *MemTable) Get(key *LookupKey, value *[]byte) (bool, Status) {
	mkey := key.MemtableKey()
	it := NewSkiplistIterator(mtbl.table)
	it.Seek(mkey)
	if it.Valid() {
		// entry format is:
		//    klength  varint32
		//    userkey  char[klength]
		//    tag      uint64
		//    vlength  varint32
		//    value    char[vlength]
		// Check that it belongs to same user key.  We do not check the
		// sequence number since the Seek() call above should have skipped
		// all entries with overly large sequence numbers.
		entry := it.Key()
		keyBuf := bytes.NewBuffer(entry[:])
		keyLen, err := GetVarint32(keyBuf)
		if err != nil {
			debug.Panicln("MemTable.Get got keyLen failed:", err)
			return false, NewStatus(IOError, err.Error())
		}

		if keyLen <= 8 {
			debug.Panicln("MemTable.Get got keyLen <= 8:", keyLen)
			return false, NewStatus(IOError, "keyLen is less than 8")
		}

		if mtbl.mcmp.InternalKeyComparator.userCmp.Compare(keyBuf.Bytes()[0:keyLen-8], key.UserKey()) == 0 {
			seqtype, _ := DecodeFixed64(bytes.NewBuffer(keyBuf.Bytes()[keyLen-8 : keyLen]))
			t := ValueType(seqtype & 0xFF)
			switch t {
			case TypeValue:
				keyBuf.Next(int(keyLen))
				if err := GetLengthPrefixedSlice(keyBuf, value); err != nil {
					debug.Panicln("MemTable.Get got value failed:", err)
				}
				return true, NewStatus(OK)
			case TypeDeletion:
				return true, NewStatus(NotFound, string(key.UserKey()))
			default:
				debug.Panicf("Wrong type %v when get key %v", t, string(key.UserKey()))
			}
		}
	}

	return false, NewStatus(NotFound)
}

type MemTableIterator struct {
	iter Iterator      // SkiplistIterator
	tmp  *bytes.Buffer // For passing to EncodeKey
}

func NewMemTableIterator(mtable *MemTable) Iterator {
	miter := &MemTableIterator{}
	miter.iter = NewSkiplistIterator(mtable.table)
	miter.tmp = bytes.NewBuffer(nil)

	return miter
}

func (mit *MemTableIterator) Valid() bool {
	return mit.iter.Valid()
}

func (mit *MemTableIterator) Seek(target []byte) {
	mit.tmp.Reset()
	PutLengthPrefixedSlice(mit.tmp, target)

	mit.iter.Seek(mit.tmp.Bytes())
}

func (mit *MemTableIterator) SeekToFirst() {
	mit.iter.SeekToFirst()
}

func (mit *MemTableIterator) SeekToLast() {
	mit.iter.SeekToLast()
}

func (mit *MemTableIterator) Next() {
	mit.iter.Next()
}

func (mit *MemTableIterator) Prev() {
	mit.iter.Prev()
}

func (mit *MemTableIterator) Key() []byte {
	buf := bytes.NewBuffer(mit.iter.Key()) // buf: keylen+8 | key content | seq+type | val len | value
	var key []byte
	if err := GetLengthPrefixedSlice(buf, &key); err != nil {
		panic(err.Error())
	}

	return key // key content + seq-type
}

func (mit *MemTableIterator) Value() []byte {
	buf := bytes.NewBuffer(mit.iter.Key()) // buf: keylen+8 | key content | seq+type | val len | value
	if klen, err := GetVarint32(buf); err != nil {
		panic(err.Error())
	} else {
		buf.Next(int(klen))
	}

	var value []byte
	if err := GetLengthPrefixedSlice(buf, &value); err != nil {
		panic(err.Error())
	}

	return value
}

func (mit *MemTableIterator) Status() Status {
	return mit.iter.Status()
}
