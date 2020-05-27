package leveldb

// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

import (
	"bytes"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()

	os.Exit(code)
}

var bloom FilterPolicy

func setup() {
	initLogger(os.Stdout)
	bloom = NewBloomFilterPolicy(10)
}

func teardown() {
}

func TestEmptyBuilder(t *testing.T) {
	fbb := &FilterBlockBuilder{}
	fbb.policy = bloom
	block := fbb.Finish()
	empty := [5]byte{4: 11}
	if !bytes.Equal(empty[:], block) {
		t.Errorf("Empty block should be {0 0 0 0 11}, but %v", block)
	}

	fbr := NewFilterBlockReader(bloom, block)
	if !fbr.KeyMayMatch(0, []byte("foo")) {
		t.Errorf("Empty block should report match")
	}
	if !fbr.KeyMayMatch(100000, []byte("foo")) {
		t.Errorf("Empty block should report match")
	}
}

func TestSingleChunk(t *testing.T) {
	fbb := &FilterBlockBuilder{}
	fbb.policy = bloom
	fbb.StartBlock(0) // no op

	fbb.StartBlock(100)
	fbb.AddKey([]byte("foo"))
	fbb.AddKey([]byte("bar"))
	fbb.AddKey([]byte("box"))

	fbb.StartBlock(200)
	fbb.AddKey([]byte("box"))

	fbb.StartBlock(300)
	fbb.AddKey([]byte("hello"))

	block := fbb.Finish()
	fbr := NewFilterBlockReader(bloom, block)
	if !fbr.KeyMayMatch(100, []byte("foo")) {
		t.Errorf("should report match")
	}
	if !fbr.KeyMayMatch(100, []byte("bar")) {
		t.Errorf("should report match")
	}
	if !fbr.KeyMayMatch(100, []byte("box")) {
		t.Errorf("should report match")
	}
	if !fbr.KeyMayMatch(100, []byte("hello")) {
		t.Errorf("should report match")
	}
	if fbr.KeyMayMatch(100, []byte("missing")) {
		t.Errorf("should report NOT match")
	}
	if fbr.KeyMayMatch(100, []byte("other")) {
		t.Errorf("should report NOT match")
	}
}

func TestMultiChunk(t *testing.T) {
	fbb := &FilterBlockBuilder{}
	fbb.policy = bloom

	// 1 filter
	fbb.StartBlock(0) // no op
	fbb.AddKey([]byte("foo"))
	fbb.StartBlock(2000)
	fbb.AddKey([]byte("bar"))

	// 2 filter
	fbb.StartBlock(3100)
	fbb.AddKey([]byte("box"))

	// 3 filter: empty
	// 4 filter

	// 5 filter
	fbb.StartBlock(9000)
	fbb.AddKey([]byte("box"))
	fbb.AddKey([]byte("hello"))

	block := fbb.Finish()
	fbr := NewFilterBlockReader(bloom, block)

	// Check first filter
	if !fbr.KeyMayMatch(0, []byte("foo")) {
		t.Errorf("should report match")
	}
	if !fbr.KeyMayMatch(2000, []byte("bar")) {
		t.Errorf("should report match")
	}
	if fbr.KeyMayMatch(0, []byte("box")) {
		t.Errorf("should report NOT match")
	}
	if fbr.KeyMayMatch(0, []byte("hello")) {
		t.Errorf("should report NOT match")
	}

	// Check second filter
	if !fbr.KeyMayMatch(3100, []byte("box")) {
		t.Errorf("should report match")
	}
	if fbr.KeyMayMatch(3100, []byte("foo")) {
		t.Errorf("should report NOT match")
	}
	if fbr.KeyMayMatch(3100, []byte("bar")) {
		t.Errorf("should report NOT match")
	}
	if fbr.KeyMayMatch(3100, []byte("hello")) {
		t.Errorf("should report NOT match")
	}

	// Check third/fourth filter (empty)
	if fbr.KeyMayMatch(4100, []byte("foo")) {
		t.Errorf("should report NOT match")
	}
	if fbr.KeyMayMatch(4100, []byte("bar")) {
		t.Errorf("should report NOT match")
	}
	if fbr.KeyMayMatch(4100, []byte("box")) {
		t.Errorf("should report NOT match")
	}
	if fbr.KeyMayMatch(4100, []byte("hello")) {
		t.Errorf("should report NOT match")
	}
	if fbr.KeyMayMatch(6100, []byte("foo")) {
		t.Errorf("should report NOT match")
	}
	if fbr.KeyMayMatch(6100, []byte("bar")) {
		t.Errorf("should report NOT match")
	}
	if fbr.KeyMayMatch(6100, []byte("box")) {
		t.Errorf("should report NOT match")
	}
	if fbr.KeyMayMatch(6100, []byte("hello")) {
		t.Errorf("should report NOT match")
	}
	// Check last filter
	if !fbr.KeyMayMatch(9000, []byte("box")) {
		t.Errorf("should report match")
	}
	if !fbr.KeyMayMatch(9000, []byte("hello")) {
		t.Errorf("should report match")
	}
	if fbr.KeyMayMatch(9000, []byte("foo")) {
		t.Errorf("should report NOT match")
	}
	if fbr.KeyMayMatch(9000, []byte("bar")) {
		t.Errorf("should report NOT match")
	}
}
