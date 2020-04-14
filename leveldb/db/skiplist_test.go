package leveldb

// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

import (
	"bytes"
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

var sl *SkipList
var kv map[string]string

func setup() {
	log.Println("setup...")

	sl = NewSkipList(NewBytewiseComparator())

	kv = map[string]string{
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

	for k, v := range kv {
		e := sl.Insert([]byte(k), []byte(v))
		if e != nil {
			log.Fatalf("Insert %v error %v", k, e)
		}
	}
}

func teardown() {
	log.Println("teardown...")
}

func TestFindGreatOrEqual(t *testing.T) {
	for k, v := range kv {
		n := sl.findGreatOrEqual([]byte(k), nil)
		if n == nil || bytes.Compare(n.value, []byte(v)) != 0 {
			t.Errorf("findGreatOrEqual %v error %v", k, n)
		}
	}
}

func TestFindGreater(t *testing.T) {
	for k, _ := range kv {
		n := sl.findGreater([]byte(k))
		if k == "n" {
			if n != nil {
				t.Errorf("findGreater %v error %v", k, n)
			}
		} else {
			if n == nil || bytes.Compare([]byte(k), n.key) >= 0 {
				t.Errorf("findGreater %v error %v", k, n)
			}
		}
	}
}

func TestFindLessOrEqual(t *testing.T) {
	for k, v := range kv {
		n := sl.findLessOrEqual([]byte(k))
		if n == nil || bytes.Compare(n.value, []byte(v)) != 0 {
			t.Errorf("findLessOrEqual %v error %v", k, n)
		}
	}
}

func TestFindLesser(t *testing.T) {
	for k, _ := range kv {
		n := sl.findLesser([]byte(k))
		if k == "a" {
			if n != nil {
				t.Errorf("findLesser %v error %v", k, n)
			}
		} else {
			if n == nil || bytes.Compare([]byte(k), n.key) <= 0 {
				t.Errorf("findLesser %v error %v", k, n)
			}
		}
	}
}

func TestContains(t *testing.T) {
	notExist := []string{
		"0",
		"x",
	}

	for _, k := range notExist {
		if sl.Contains([]byte(k)) {
			t.Errorf("should not contains %v", k)
		}
	}

	for k, _ := range kv {
		if !sl.Contains([]byte(k)) {
			t.Errorf("should contains %v", k)
		}
	}
}

func TestNotExist(t *testing.T) {
	notExist := []string{
		"0",
		"x",
	}

	greaterOrEqual := []string{
		"a",
		"",
	}

	// test findGreatOrEqual
	for i, k := range notExist {
		n := sl.findGreatOrEqual([]byte(k), nil)
		n_is_nil := (n == nil)
		expect_is_nil := (greaterOrEqual[i] == "")

		if n_is_nil != expect_is_nil {
			t.Errorf("findGreatOrEqual %v error %v", k, n)
		}
		if !n_is_nil && string(n.key) != greaterOrEqual[i] {
			t.Errorf("findGreatOrEqual %v error %v", k, n)
		}
	}

	lessOrEqual := []string{
		"",
		"n",
	}
	// test findLessOrEqual
	for i, k := range notExist {
		n := sl.findLessOrEqual([]byte(k))
		n_is_nil := (n == nil)
		expect_is_nil := (lessOrEqual[i] == "")

		if n_is_nil != expect_is_nil {
			t.Errorf("findLessOrEqual %v error %v", k, n)
		}
		if !n_is_nil && string(n.key) != lessOrEqual[i] {
			t.Errorf("findLessOrEqual %v error %v", k, n)
		}
	}
}

func TestIterator(t *testing.T) {
	keys := make([]string, 0, len(kv))
	for k, _ := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// traverse skiplist
	it := NewSkiplistIterator(sl)
	for _, k := range keys {
		if !it.Valid() {
			t.Fatalf("Invalid iterator when key = %v", k)
		}

		log.Printf("key %v\n", string(it.Key()))

		if string(it.Key()) != k {
			t.Fatalf("Iterator key %v != %v", string(it.Key()), k)
		}

		it.Next()
	}

	// seek first
	it.SeekToFirst()
	if string(it.Key()) != keys[0] {
		t.Fatalf("First key %v != %v", string(it.Key()), keys[0])
	}

	// seek
	it.Seek([]byte("n"))
	if string(it.Key()) != keys[len(keys)-1] {
		t.Fatalf("Seek 'n' key %v != %v", string(it.Key()), keys[len(keys)-1])
	}

	it.Seek([]byte("z"))
	if it.Valid() {
		t.Fatalf("Should be invalid() after Seek 'z'")
	}

	it.Seek([]byte("a"))
	if string(it.Key()) != keys[0] {
		t.Fatalf("Seek 'a' key %v != %v", string(it.Key()), keys[0])
	}

	// seek last
	it.SeekToLast()
	if string(it.Key()) != keys[len(keys)-1] {
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

		if string(it.Key()) != k {
			t.Fatalf("Reverse iterator key %v != %v", string(it.Key()), k)
		}

		it.Prev()
	}
}

func TestEmptyIterator(t *testing.T) {
	it := NewSkiplistIterator(nil)
	if it.Valid() {
		t.Fatalf("Should be invalid() for nil list")
	}

	sk := NewSkipList(NewBytewiseComparator())
	it = NewSkiplistIterator(sk)
	if it.Valid() {
		t.Fatalf("Should be invalid() for empty list")
	}
}

func BenchmarkFindGreaterOrEqual(b *testing.B) {
	keys := [][]byte{
		[]byte("0"),
		[]byte("1"),
		[]byte("2"),
		[]byte("3"),
		[]byte("4"),
		[]byte("5"),
		[]byte("a"),
		[]byte("b"),
		[]byte("c"),
		[]byte("d"),
	}
	for i := 0; i < b.N; i++ {
		for _, k := range keys {
			_ = sl.findGreatOrEqual(k, nil)
		}
	}
}

func BenchmarkInsert(b *testing.B) {
	keys := [][]byte{
		[]byte("0"),
		[]byte("1"),
		[]byte("2"),
		[]byte("3"),
		[]byte("4"),
		[]byte("5"),
		[]byte("a"),
		[]byte("b"),
		[]byte("c"),
		[]byte("d"),
	}
	for i := 0; i < b.N; i++ {
		slist := NewSkipList(NewBytewiseComparator())
		for _, v := range keys {
			_ = slist.Insert(v, v)
		}
	}
}
