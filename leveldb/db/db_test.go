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

var db *DB

func setup() {
	log.Println("setup...")
	db, _ = Open(NewOptions(), "tmpdb")
}

func teardown() {
	log.Println("teardown...")
}

func TestWrite(t *testing.T) {
	wopt := WriteOptions{}
	ropt := NewReadOptions()

	var tests = []struct {
		t     ValueType
		key   []byte
		value []byte
	}{
		{TypeValue, []byte("city"), []byte("shanghai")},
		{TypeValue, []byte("city"), []byte("shenzhen")},
		{TypeDeletion, []byte("city"), nil},
	}

	for _, v := range tests {
		if v.t == TypeValue {
			db.Put(&wopt, v.key, v.value)
		} else if v.t == TypeDeletion {
			db.Delete(&wopt, v.key)
		}
	}

	// Default db sequence is started at 1
	t.Logf("Try look up for city at diff version\n")
	for seq := 1; seq <= 5; seq++ {
		ropt.Snapshot = SequenceNumber(seq)
		value, st := db.Get(ropt, []byte("city"))

		succ := true
		switch seq {
		case 1, 2:
			succ = true
		case 3, 4, 5:
			succ = false
		default:
			panic("never here")
		}

		if succ != st.IsOK() {
			t.Errorf("Seq %d, err %v", seq, st)
		} else if st.IsOK() {
			t.Logf("Find city at seq %d: %v\n", seq, string(value))
		}
	}
}

func TestWriteBatch(t *testing.T) {
	db, _ = Open(NewOptions(), "tmpdb")

	wopt := WriteOptions{}
	ropt := NewReadOptions()

	var tests = []struct {
		t     ValueType
		key   []byte
		value []byte
	}{
		{TypeValue, []byte("city"), []byte("shanghai2")},
		{TypeValue, []byte("city"), []byte("shenzhen2")},
		{TypeDeletion, []byte("city"), nil},
	}

	batch := NewWriteBatch()
	for _, v := range tests {
		if v.t == TypeValue {
			batch.Put(v.key, v.value)
		} else if v.t == TypeDeletion {
			batch.Delete(v.key)
		}
	}

	if st := db.Write(&wopt, batch); !st.IsOK() {
		t.Errorf("Write batch failed %v", st)
	}

	// Default db sequence is started at 1
	t.Logf("Try look up for city at diff version\n")
	for seq := 1; seq <= 5; seq++ {
		ropt.Snapshot = SequenceNumber(seq)
		value, st := db.Get(ropt, []byte("city"))

		succ := true
		switch seq {
		case 1, 2:
			succ = true
		case 3, 4, 5:
			succ = false
		default:
			panic("never here")
		}

		if succ != st.IsOK() {
			t.Errorf("Seq %d, err %v", seq, st)
		} else if st.IsOK() {
			t.Logf("Find city at seq %d: %v\n", seq, string(value))
		}
	}
}
