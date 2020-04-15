package leveldb

// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Features:
// (1) Not support delete
// (2) Not support duplicated keys
// (3) One writer and many readers can be concurrent

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//

import (
	"fmt"

	"bytes"
	"math/rand"
	"sync/atomic"
	"time"
	"unsafe"
)

// internal node for skip list
type node struct {
	key   []byte
	value []byte
	next  []*node
}

func (n *node) String() string {
	return fmt.Sprintf("(key:%s, value:%s, next %v)", n.key, n.value, n.next)
}

func (n *node) loadNext(i int) *node {
	up := unsafe.Pointer(n.next[i])
	return (*node)(atomic.LoadPointer(&up))
}

func (n *node) storeNext(i int, next *node) {
	up := unsafe.Pointer(&n.next[i])
	atomic.StorePointer((*unsafe.Pointer)(up), unsafe.Pointer(next))
}

func (n *node) getNext(i int) *node {
	return n.next[i]
}

func (n *node) setNext(i int, next *node) {
	n.next[i] = next
}

type SkipList struct {
	head   node
	height int32 // 0-based
	rnd    *rand.Rand
	cmp    Comparator

	numNode  int
	byteSize int64 // key size + value size
}

func NewSkipList(cmp Comparator) *SkipList {
	sl := &SkipList{}

	sl.head.next = make([]*node, sl.MaxHeight()+1)
	sl.height = 0
	sl.cmp = cmp
	sl.rnd = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	sl.numNode = 0
	sl.byteSize = 0

	if sl.cmp == nil {
		sl.cmp = NewBytewiseComparator()
	}

	return sl
}

func (sl *SkipList) NumOfNode() int {
	return sl.numNode
}

func (sl *SkipList) ByteSize() int64 {
	return sl.byteSize
}

func (sl *SkipList) Height() int32 {
	return atomic.LoadInt32(&sl.height)
}

const (
	kMaxHeight = 11
)

func (sl *SkipList) MaxHeight() int32 {
	return kMaxHeight // 0-based
}

func (sl *SkipList) randomHeight() int32 {
	const kBranching = 4
	var h int32 = 0
	for ; h <= sl.MaxHeight(); h++ {
		if sl.rnd.Int()%kBranching != 0 {
			return h
		}
	}

	return sl.MaxHeight()
}

func compareKeyNode(key []byte, n *node, cmp Comparator) int {
	if n == nil {
		return cmp.Compare(key, nil)
	}

	return cmp.Compare(key, n.key)
}

// single thread insert
func (sl *SkipList) Insert(key, value []byte) error {
	prev := [kMaxHeight + 1]*node{}
	ge := sl.findGreatOrEqual(key, &prev)

	if ge != nil && sl.cmp.Compare(key, ge.key) == 0 {
		return fmt.Errorf("Repeated key not allowed: [%s]", key)
	}

	h := sl.randomHeight()
	if h > sl.Height() {
		for i := sl.Height() + 1; i <= h; i++ {
			prev[i] = &sl.head
		}

		// Update height first. It's ok with concurrent readers.
		// A concurrent reader that observes the new value of height will see either the old value of
		// new level pointers from head (nil), or a new value set in
		// the loop below.  In the former case the reader will
		// immediately drop to the next level since nil sorts after all
		// keys.  In the latter case the reader will use the new node.
		atomic.StoreInt32(&sl.height, h)
	}

	x := &node{key: key, value: value}
	x.next = make([]*node, h+1)

	// 从底向上将新节点链接进去；因为高层的节点必须在底层也存在，反之不一定
	for i := 0; i <= int(h); i++ {
		x.setNext(i, prev[i].getNext(i)) // no need barrier, x is still dangle
		prev[i].storeNext(i, x)          // commit x into skiplist
	}

	sl.numNode++
	sl.byteSize += int64(len(key) + len(value))
	return nil
}

func (sl *SkipList) Contains(key []byte) bool {
	ge := sl.findGreatOrEqual(key, nil)

	if ge != nil && sl.cmp.Compare(key, ge.key) == 0 {
		return true
	}

	return false
}

// similar to std::map::lower_bound
func (sl *SkipList) findGreatOrEqual(key []byte, prev *[kMaxHeight + 1]*node) *node {
	level := int(sl.Height())
	x := &sl.head

	for {
		next := x.loadNext(level)
		cmp := compareKeyNode(key, next, sl.cmp)
		if cmp > 0 {
			x = next
		} else if cmp < 0 {
			// next maybe nil
			if prev != nil {
				prev[level] = x
			}

			if level == 0 {
				return next
			} else {
				level--
			}
		} else {
			return next
		}
	}

	return nil
}

// similar to std::map::upper_bound
func (sl *SkipList) findGreater(key []byte) *node {
	n := sl.findGreatOrEqual(key, nil)
	if n != nil {
		cmp := sl.cmp.Compare(key, n.key)
		if cmp == 0 {
			return n.next[0]
		} else if cmp > 0 {
			panic("fuck me")
		}
	}

	return n
}

func (sl *SkipList) findLessOrEqual(key []byte) *node {
	level := int(sl.Height())
	x := &sl.head

	for {
		next := x.loadNext(level)
		cmp := compareKeyNode(key, next, sl.cmp)
		if cmp > 0 {
			x = next
		} else if cmp < 0 {
			// next maybe nil
			if level == 0 {
				if x == &sl.head {
					x = nil
				}
				return x
			} else {
				level--
			}
		} else {
			return next
		}
	}

	return nil
}

func (sl *SkipList) findLesser(key []byte) *node {
	level := int(sl.Height())
	x := &sl.head

	for {
		next := x.loadNext(level)
		cmp := compareKeyNode(key, next, sl.cmp)
		if cmp > 0 {
			x = next
		} else if cmp <= 0 {
			// next maybe nil
			if level == 0 {
				if x == &sl.head {
					x = nil
				}

				return x
			} else {
				level--
			}
		}
	}

	return nil
}

func (sl *SkipList) first() *node {
	return sl.head.getNext(0)
}

func (sl *SkipList) last() *node {
	level := int(sl.Height())
	prev := sl.head.getNext(level)
	for level >= 0 {
		if prev == nil {
			return nil
		}
		next := prev.getNext(level)
		for next != nil {
			prev = next
			next = next.getNext(level)
		}
		if level == 0 {
			return prev
		} else {
			level--
		}
	}

	// never reach here
	return nil
}

func (sl *SkipList) String() string {
	if sl == nil {
		return "SkipList (nil)"
	}

	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("\nSkiplist Height: %d,  Num of node %d,  Byte size %v\n", sl.height, sl.numNode, sl.byteSize))
	for i := sl.height; i >= 0; i-- {
		buf.WriteString(fmt.Sprintf("Level %d ----------------------------------\n", i))
		i := int(i)
		for n := sl.head.getNext(i); n != nil; n = n.getNext(i) {
			buf.WriteString(fmt.Sprintf("%v -> ", n.key))
		}
		buf.WriteString("(nil)\n")
	}

	return buf.String()
}

type SkiplistIterator struct {
	sklist  *SkipList
	current *node // point to min value at first
	state   Status
}

func NewSkiplistIterator(sk *SkipList) Iterator {
	it := &SkiplistIterator{sklist: sk}
	if sk != nil {
		it.SeekToFirst()
	}

	return it
}

func (it *SkiplistIterator) Valid() bool {
	return it.current != nil
}

func (it *SkiplistIterator) SeekToFirst() {
	it.current = it.sklist.first()
	if it.current != nil {
		it.state = NewStatus(OK)
	} else {
		it.state = NewStatus(IOError, "Empty skiplist")
	}
}

func (it *SkiplistIterator) SeekToLast() {
	it.current = it.sklist.last()
	if it.current == nil {
		it.state = NewStatus(IOError, "Empty skiplist")
	} else {
		it.state = NewStatus(OK)
	}
}

func (it *SkiplistIterator) Seek(target []byte) {
	ge := it.sklist.findGreatOrEqual(target, nil)
	it.current = ge
}

func (it *SkiplistIterator) Next() {
	next := it.sklist.findGreater(it.current.key)
	it.current = next
}

func (it *SkiplistIterator) Prev() {
	prev := it.sklist.findLesser(it.current.key)
	it.current = prev
}

func (it *SkiplistIterator) Key() []byte {
	return it.current.key
}

func (it *SkiplistIterator) Value() []byte {
	return it.current.value
}

func (it *SkiplistIterator) Status() Status {
	return it.state
}
