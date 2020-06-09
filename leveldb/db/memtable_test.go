package leveldb

// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

import (
	"log"
	"os"
	"sort"
	"testing"
)

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()

	os.Exit(code)
}

var tbl *MemTable

func setup() {
	initLogger(os.Stdout)
	log.Println("setup...")
	comparator := NewBytewiseComparator()
	icmp := &InternalKeyComparator{userCmp: comparator}
	tbl = NewMemtable(icmp)
}

func teardown() {
	log.Println("teardown...")
}

func TestAddGet(t *testing.T) {
	var tests = []struct {
		seq   SequenceNumber
		t     ValueType
		key   []byte
		value []byte
	}{
		{1, TypeValue, []byte("city"), []byte("shanghai")},
		{2, TypeValue, []byte("city"), []byte("shenzhen")},
		{3, TypeDeletion, []byte("city"), nil},
	}

	for _, v := range tests {
		tbl.Add(v.seq, v.t, v.key, v.value)
	}

	t.Logf("Try look up for city at diff version\n")
	for seq := 0; seq < 5; seq++ {
		lkey := NewLookupKey([]byte("city"), SequenceNumber(seq))
		var value []byte
		ok, st := tbl.Get(lkey, &value)

		expect := true
		haveState := false
		switch seq {
		case 0:
			expect = false
		case 1, 2:
			haveState = true
		case 3, 4:
			haveState = true
		default:
			panic("never here")
		}

		if ok != expect || (st != nil) != haveState {
			t.Errorf("Seq %d, ok %v, st %v", seq, ok, st)
		} else if ok && st == nil {
			t.Logf("Find city at seq %d: %v\n", seq, string(value))
		}
	}
}

func TestIterator(t *testing.T) {
	mtbl := NewMemtable(&InternalKeyComparator{userCmp: NewBytewiseComparator()})
	kv := map[string]string{
		"a": "aaa",
		"b": "bbb",
		"c": "ccc",
		"d": "ddd",
		"e": "eee",
		"f": "fff",
		"g": "ggg",
		"h": "hhh",
		"i": "iii",
		"j": "jjj",
		"k": "kkk",
		"l": "lll",
		"m": "mmm",
		"n": "nnn",
	}

	var seq SequenceNumber = 1
	for k, v := range kv {
		mtbl.Add(seq, TypeValue, []byte(k), []byte(v))
		seq++
	}

	keys := make([]string, 0, len(kv))
	for k, _ := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// traverse memtable
	it := NewMemTableIterator(mtbl)
	for _, k := range keys {
		if !it.Valid() {
			t.Fatalf("Invalid iterator when key = %v", k)
		}

		//log.Printf("key %v\n", string(it.Key()))

		if string(ExtractUserKey(it.Key())) != k {
			t.Fatalf("Iterator key %v != %v", string(it.Key()), k)
		}
		if string(it.Value()) != kv[k] {
			t.Fatalf("Iterator value %v != %v", string(it.Value()), kv[k])
		}

		it.Next()
	}

	// seek first
	it.SeekToFirst()
	if string(ExtractUserKey(it.Key())) != keys[0] {
		t.Fatalf("First key %v != %v", string(it.Key()), keys[0])
	}

	// seek
	it.Seek(AppendInternalKey(NewParsedInternalKey([]byte("n"), kMaxSequenceNumber, TypeValueForSeek)))
	if string(ExtractUserKey(it.Key())) != keys[len(keys)-1] {
		t.Fatalf("Seek 'n' key %v != %v", string(it.Key()), keys[len(keys)-1])
	}

	it.Seek(AppendInternalKey(NewParsedInternalKey([]byte("z"), kMaxSequenceNumber, TypeValueForSeek)))
	if it.Valid() {
		t.Fatalf("Should be invalid() after Seek 'z'")
	}

	it.Seek(AppendInternalKey(NewParsedInternalKey([]byte("a"), kMaxSequenceNumber, TypeValueForSeek)))
	if string(ExtractUserKey(it.Key())) != keys[0] {
		t.Fatalf("Seek 'a' key %v != %v", string(it.Key()), keys[0])
	}

	// seek last
	it.SeekToLast()
	if string(ExtractUserKey(it.Key())) != keys[len(keys)-1] {
		t.Fatalf("Last key %v != %v", string(it.Key()), keys[len(keys)-1])
	}

	// reverse traverse skiplist
	it.SeekToLast()
	sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	for _, k := range keys {
		if !it.Valid() {
			t.Fatalf("Invalid reverse iterator when key = %v", k)
		}

		log.Printf("Reverse key %v\n", string(it.Key()))

		if string(ExtractUserKey(it.Key())) != k {
			t.Fatalf("Reverse iterator key %v != %v", string(it.Key()), k)
		}

		it.Prev()
	}

	t.Logf("Try look up for city at diff version\n")
	for seq := 0; seq < 5; seq++ {
		lkey := NewLookupKey([]byte("city"), SequenceNumber(seq))
		var value []byte
		ok, st := tbl.Get(lkey, &value)

		expect := true
		haveState := false
		switch seq {
		case 0:
			expect = false
		case 1, 2:
			haveState = true
		case 3, 4:
			haveState = true
		default:
			panic("never here")
		}

		if ok != expect || (st != nil) != haveState {
			t.Errorf("Seq %d, ok %v, st %v", seq, ok, st)
		} else if ok && st == nil {
			t.Logf("Find city at seq %d: %v\n", seq, string(value))
		}
	}
}
