package leveldb

// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

import (
	"log"
	"os"
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
